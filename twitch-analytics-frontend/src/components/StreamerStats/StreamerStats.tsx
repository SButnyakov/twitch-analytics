import { useStreamerStats } from "../../hooks/useStreamerStats";
import { StreamerStatsProps } from "../../types/streamerStats";

const StreamerStats: React.FC<StreamerStatsProps> = ({id}) => {
    const { streamerStats, error } = useStreamerStats(id)

    if (error !== null) {
        return (
            <h1>{error}</h1>
        )
    }

    if (streamerStats === null) {
        return (
            <h1>Not found</h1>
        )
    }

    return (
        <div>
            <h1>Name: {streamerStats.name}</h1>
            <p>Average online: {streamerStats.avgonline}</p>
            <p>Global rank: {streamerStats.globalrank}</p>
            <p>Local rank: {streamerStats.languagerank}</p>
            <p>Top game: {streamerStats.topgameid}</p>
        </div>
    )
}

export default StreamerStats;