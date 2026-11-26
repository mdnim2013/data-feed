/**
 * Network Mesh Topology & Node Validation Engine (v3 Schema)
 * High-performance sharded crawler for TrendClip
 */

const fs = require('fs');
const path = require('path');

const SHARD_ID = process.env.SHARD_ID || '1';
const TEST_NICHE = process.env.TEST_NICHE || '';
const REGION_CODE = (process.env.REGION_CODE || 'US').toUpperCase();
const CRAWL_START = Date.now();

const crawlStats = {
    duration_ms: 0,
    requests_total: 0,
    http_429_count: 0,
    failed_niches: []
};

console.log(TEST_NICHE
    ? `[Mesh Validator v3] Starting Single-Niche Test Crawl for "${TEST_NICHE}" (${REGION_CODE})...`
    : `[Mesh Validator v3] Starting Shard #${SHARD_ID} (${REGION_CODE})...`
);

// 1. Load Node Specifications (Topics & Keywords)
let topicsFile = path.join(__dirname, 'node_specs.json');
if (!fs.existsSync(topicsFile)) topicsFile = path.join(__dirname, 'nodes_manifest.json');
if (!fs.existsSync(topicsFile)) topicsFile = path.join(__dirname, 'topics.json');
if (!fs.existsSync(topicsFile)) {
    console.error('FATAL: No topics manifest found.');
    process.exit(1);
}

const allTopics = JSON.parse(fs.readFileSync(topicsFile, 'utf8'));
if (!Array.isArray(allTopics)) {
    console.error('FATAL: Invalid topics configuration format.');
    process.exit(1);
}

let activeTopics;
if (TEST_NICHE) {
    activeTopics = allTopics.filter(t => t.id === TEST_NICHE);
    if (!activeTopics.length) {
        console.error(`FATAL: Test niche "${TEST_NICHE}" not found.`);
        process.exit(1);
    }
} else {
    activeTopics = allTopics.filter(t => (t.enabled !== false) && Array.isArray(t.keywords) && t.keywords.length > 0);
    const hasMultipleShards = activeTopics.some(t => t.shard && t.shard !== parseInt(SHARD_ID, 10));
    if (hasMultipleShards) {
        activeTopics = activeTopics.filter(t => parseInt(t.shard, 10) === parseInt(SHARD_ID, 10));
    } else if (!activeTopics.every(t => t.shard)) {
        activeTopics = activeTopics.filter((t, idx) => (idx % 6) + 1 === parseInt(SHARD_ID, 10));
    }
}

const totalKeywords = activeTopics.reduce((acc, t) => acc + t.keywords.length, 0);
console.log(`[Mesh Validator v3] Loaded ${activeTopics.length} niches, ${totalKeywords} keywords.`);

// 2. Helpers
function sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
}

function parseDurationSec(str) {
    if (!str) return 0;
    const parts = String(str).trim().split(':').map(n => parseInt(n, 10) || 0);
    if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
    if (parts.length === 2) return parts[0] * 60 + parts[1];
    if (parts.length === 1) return parts[0];
    return 0;
}

function parseAgeTextToHours(text) {
    if (!text) return null;
    const match = text.replace(/^Streamed\s+/i, '').match(/(\d+)\s*(second|minute|hour|day|week|month|year)/i);
    if (!match) return null;
    const n = parseInt(match[1], 10);
    const mult = { second: 1 / 3600, minute: 1 / 60, hour: 1, day: 24, week: 168, month: 720, year: 8760 };
    return n * (mult[match[2].toLowerCase()] || 0);
}

async function fetchWithBackoff(url, options, maxRetries = 3) {
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
        crawlStats.requests_total++;
        const resp = await fetch(url, options);
        if (resp.status === 429) {
            crawlStats.http_429_count++;
            if (attempt === maxRetries) throw new Error('RATE_LIMIT');
            const jitter = Math.floor(Math.random() * 500);
            await sleep(Math.pow(2, attempt) * 1000 + jitter);
            continue;
        }
        return resp;
    }
    throw new Error('RATE_LIMIT');
}

// 3. YouTube InnerTube Bootstrap
async function getYTConfig() {
    const res = await fetch('https://www.youtube.com/', {
        headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        }
    });
    const html = await res.text();
    const apiKey = html.match(/"INNERTUBE_API_KEY"\s*:\s*"([^"]+)"/)?.[1];
    const clientVersion = html.match(/"INNERTUBE_CLIENT_VERSION"\s*:\s*"([^"]+)"/)?.[1];
    if (!apiKey || !clientVersion) {
        throw new Error('Could not extract InnerTube credentials.');
    }
    return { apiKey, clientVersion };
}

async function fetchSearch(config, query, filterParam = null, maxPages = 1) {
    const results = [];
    let continuationToken = null;

    for (let page = 0; page < maxPages; page++) {
        const body = {
            context: {
                client: {
                    clientName: 'WEB',
                    clientVersion: config.clientVersion,
                    hl: 'en',
                    gl: REGION_CODE
                }
            }
        };
        if (continuationToken) body.continuation = continuationToken;
        else {
            body.query = query;
            if (filterParam) body.params = filterParam;
        }

        try {
            const resp = await fetchWithBackoff(
                `https://www.youtube.com/youtubei/v1/search?key=${config.apiKey}&prettyPrint=false`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-YouTube-Client-Name': '1',
                        'X-YouTube-Client-Version': config.clientVersion,
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
                    },
                    body: JSON.stringify(body)
                }
            );
            const data = await resp.json();
            const contents = data.contents?.twoColumnSearchResultsRenderer?.primaryContents?.sectionListRenderer?.contents ||
                             data.onResponseReceivedCommands?.[0]?.appendContinuationItemsAction?.continuationItems || [];

            const videoItems = [];
            let nextToken = null;

            for (const c of contents) {
                if (c.itemSectionRenderer?.contents) videoItems.push(...c.itemSectionRenderer.contents);
                else if (c.continuationItemRenderer?.continuationEndpoint?.continuationCommand?.token) {
                    nextToken = c.continuationItemRenderer.continuationEndpoint.continuationCommand.token;
                }
            }

            for (const item of videoItems) {
                const vr = item.videoRenderer;
                if (!vr || !vr.videoId) continue;

                const ownerRun = vr.ownerText?.runs?.[0] || {};
                const browseId = ownerRun.navigationEndpoint?.browseEndpoint?.browseId || '';
                const canonicalUrl = ownerRun.navigationEndpoint?.browseEndpoint?.canonicalBaseUrl || '';
                const handle = canonicalUrl.startsWith('/@') ? canonicalUrl.slice(1) : null;

                results.push({
                    videoId: vr.videoId,
                    title: vr.title?.runs?.[0]?.text || '',
                    channelId: browseId,
                    channelHandle: handle,
                    channelName: ownerRun.text || 'Unknown Creator',
                    viewsText: vr.viewCountText?.simpleText || '0 views',
                    publishedText: vr.publishedTimeText?.simpleText || '',
                    duration: vr.lengthText?.simpleText || '0:00'
                });
            }

            if (!nextToken) break;
            continuationToken = nextToken;
            await sleep(400);
        } catch (err) {
            if (err.message === 'RATE_LIMIT') throw err;
            break;
        }
    }
    return results;
}

// 4. Execution Pipeline
async function run() {
    const config = await getYTConfig();
    const v2Feed = {};
    const v3Videos = [];
    const v3Channels = new Map();
    const capturedAt = new Date().toISOString();
    let totalProcessed = 0;

    const CHUNK_SIZE = 10;

    for (const topic of activeTopics) {
        const catId = topic.id;
        const keywords = topic.keywords;
        const topicVideos = new Map();
        const topicV3 = new Map();

        for (let i = 0; i < keywords.length; i += CHUNK_SIZE) {
            const chunk = keywords.slice(i, i + CHUNK_SIZE);

            await Promise.all(chunk.map(async (kw) => {
                let items = [];
                try {
                    const WEEK_FILTER = 'EgIIAw==';
                    const [wRes, gRes] = await Promise.all([
                        fetchSearch(config, kw, WEEK_FILTER, 1).catch(() => []),
                        fetchSearch(config, kw, null, 1).catch(() => [])
                    ]);
                    items = [...wRes, ...gRes];
                } catch (e) {
                    if (e.message === 'RATE_LIMIT') {
                        await sleep(2000);
                    }
                    return;
                }

                items.forEach((v, rankIdx) => {
                    if (!v.videoId || topicVideos.has(v.videoId)) return;

                    const rawViews = parseInt(String(v.viewsText).replace(/[^0-9]/g, ''), 10) || 0;
                    const ageHours = parseAgeTextToHours(v.publishedText) || 720;
                    const vph = (ageHours > 0 && rawViews > 0) ? Math.round(rawViews / ageHours) : 0;
                    const velocity = Number(Math.max(1.0, Math.min(8.5, (vph / 500) + 1.0)).toFixed(1));
                    const score = Number((Math.log10(Math.max(1, vph)) * 30).toFixed(2));
                    const momentum = Math.round(Math.min(10, Math.max(1, velocity * 1.5)));
                    const durSec = parseDurationSec(v.duration);
                    const format = (durSec > 0 && durSec <= 62) ? 'short' : 'long';
                    const chId = v.channelId || `legacy_${Buffer.from(v.channelName).toString('hex').slice(0, 20)}`;

                    if (!v3Channels.has(chId)) {
                        v3Channels.set(chId, {
                            channel_id: chId,
                            handle: v.channelHandle,
                            name: v.channelName,
                            subscriber_count: null,
                            recent_view_samples: []
                        });
                    }
                    if (rawViews > 0) {
                        const chObj = v3Channels.get(chId);
                        if (chObj.recent_view_samples.length < 20) {
                            chObj.recent_view_samples.push(rawViews);
                        }
                    }

                    topicVideos.set(v.videoId, [
                        v.videoId,
                        v.title,
                        v.channelName,
                        rawViews,
                        v.publishedText,
                        v.duration,
                        vph,
                        score,
                        momentum,
                        vph
                    ]);

                    topicV3.set(v.videoId, {
                        video_id: v.videoId,
                        channel_id: chId,
                        channel_name: v.channelName,
                        niche_slug: catId,
                        title: v.title,
                        views: rawViews,
                        vph,
                        duration_sec: durSec,
                        duration_text: v.duration,
                        format,
                        published_text: v.publishedText,
                        keyword_rank: rankIdx + 1,
                        source_keyword: kw,
                        thumbnail_url: `https://i.ytimg.com/vi/${v.videoId}/hqdefault.jpg`
                    });
                });
            }));

            totalProcessed += chunk.length;
            await sleep(250);
        }

        const sortedV2 = Array.from(topicVideos.values()).sort((a, b) => (b[6] || 0) - (a[6] || 0)).slice(0, 150);
        v2Feed[catId] = sortedV2;
        if (sortedV2.length === 0) crawlStats.failed_niches.push(catId);

        const keptIds = new Set(sortedV2.map(r => r[0]));
        for (const [vid, vObj] of topicV3.entries()) {
            if (keptIds.has(vid)) v3Videos.push(vObj);
        }
    }

    crawlStats.duration_ms = Date.now() - CRAWL_START;

    const v3Payload = {
        schema_version: 3,
        shard_id: `shard-${SHARD_ID}`,
        workflow: 'mesh-audit.yml',
        region_code: REGION_CODE,
        captured_at: capturedAt,
        crawl_stats: crawlStats,
        channels: Array.from(v3Channels.values()),
        videos: v3Videos
    };

    fs.writeFileSync(path.join(__dirname, 'validator_data.json'), JSON.stringify(v2Feed));
    fs.writeFileSync(path.join(__dirname, 'validator_data_v3.json'), JSON.stringify(v3Payload));
    console.log(`[Mesh Validator v3] Done in ${crawlStats.duration_ms}ms | Videos: ${v3Videos.length} | Channels: ${v3Channels.size} | 429s: ${crawlStats.http_429_count}`);
}

run().catch(err => {
    console.error('FATAL Execution Error:', err);
    process.exit(1);
});
