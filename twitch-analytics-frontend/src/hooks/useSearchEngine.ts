import { useEffect } from "react";

export const useSearchEngine = (searchString: string, top: number, delay: number = 500) => {
    useEffect(() => {
        const getData = setTimeout(() => {
            console.log(searchString, top);
        }, delay);

        return () => clearTimeout(getData);
    }, [searchString, top]);
};
