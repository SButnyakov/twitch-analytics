import { useEffect, useState } from "react"
import { fetchStreamerStatsById } from "../api/searchEngineService";
import { StreamerStatsInfo } from "../types/streamerStats";

export const useStreamerStats = (id: number) => {
    const [streamerStats, setStreamerStats] = useState<StreamerStatsInfo | null>(null);
    const [error, setError] = useState<string|null>(null);

    useEffect(() => {
        const fetchData = setTimeout(async () => {
            setError(null);

            try {
                const data = await fetchStreamerStatsById(id);
                setStreamerStats(data);
            } catch (error: any) {
                setStreamerStats(null);
                setError(error.message);
            }
        })

        return () => clearTimeout(fetchData);
    }, [id])

    return { streamerStats, error }
};
