import { useEffect, useState } from "react";
import { fetchGamesAndStreamers } from "../api/searchEngineService";

export const useSearchEngine = (searchString: string, top: number, delay: number = 500) => {
    const [result, setResult] = useState<any>({ games: null, streamers: null });
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const fetchData = setTimeout(async () => {
            if (searchString === '') { 
                setResult({ games: null, streamers: null })
                return; 
            }

            setIsLoading(true);
            setError(null);

            try {
                const data = await fetchGamesAndStreamers(searchString, top);
                setResult(data);
            } catch (error: any) {
                setResult(null);
                setError(error.message);
            } finally {
                setIsLoading(false);
            }

        }, delay);

        return () => clearTimeout(fetchData);
    }, [searchString, top]);

    return { result, isLoading, error }
};
