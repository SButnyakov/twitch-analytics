import { useEffect, useState } from 'react'
import { fetchTimepoints } from '../api/searchEngineService';
import { TimepointsGraphResponse } from '../types/timepointsGraph';

export const useTimepointsGraph = (id: number, days: number, type: string) => {
    const [timepoints, setTimepoints] = useState<TimepointsGraphResponse | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const fetchData = setTimeout(async () => {
            setIsLoading(true);
            setError(null);

            try {
                const data = await fetchTimepoints(id, days, type);
                setTimepoints(data);
            } catch (error: any) {
                setTimepoints(null)
                setError(error.message);
            } finally {
                setIsLoading(false);
            }
        })

        return () => clearTimeout(fetchData);
    }, [days, id, type])

    return { timepoints, isLoading, error }
};
