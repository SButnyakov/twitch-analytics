export type StreamerStatsProps = {
    id: StreamerStatsId
}
export type StreamerStatsId = number;

export type StreamerStatsInfo = {
    id: StreamerStatsId
    name: StreamerStatsName
    avgonline: StreamerStatsAvgViewers
    globalrank: StreamerStatsGlobalRank
    languagerank: StreamerStatsLanguageRank
    topgameid: StreamerStatsTopGameId
}
export type StreamerStatsName = string;
export type StreamerStatsAvgViewers = number;
export type StreamerStatsGlobalRank = number;
export type StreamerStatsLanguageRank = number;
export type StreamerStatsTopGameId = number;

