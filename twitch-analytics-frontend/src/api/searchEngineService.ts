
import axios from 'axios';
import { config } from '../config/config';

const apiClient = axios.create({
    baseURL: config.BASE_API_HOST,
    timeout: config.API_TIMEOUT,
});

export const fetchGamesAndStreamers = async (searchString: string, top: number) => {
    try {
        const response = await apiClient.get('/search', {
            params: {
                q: searchString,
                top: top,
            },
        });

        return response.data;
    } catch (error) {
        console.error('Error fetcing data:', error);
        throw new Error('Failed to fetch games and streams');
    }
};

export const fetchNameById = async (id: number, type: string) => {
    try {
        const response = await apiClient.get('/search', {
            params: {
                id: id,
                t: type,
            },
        });

        return response.data;
    } catch (error) {
        console.error('Error fetcing data:', error);
        throw new Error('Failed to fetch name by id');
    }
};

export const fetchTimepoints = async (id: number, days: number, type: string) => {
    try {
        const response = await apiClient.get(`/timepoints/${type}s/${id}`, {
            params: {
                days: days
            }
        });

        return response.data;
    } catch (error) {
        console.error('Error fetching data:', error);
        throw new Error('Failed to fetch game\'s timepoints for 7 days');
    }
}

export const fetchStreamerStatsById = async (id: number) => {
    try {
        const response = await apiClient.get(`/stats/streamers/${id}`);
        return response.data;
    } catch (error) {
        console.error('Error fetcing data:', error);
        throw new Error('Failed to fetch name by id');
    }
};

