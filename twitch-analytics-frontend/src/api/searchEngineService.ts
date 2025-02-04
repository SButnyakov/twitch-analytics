
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

export const fetchNameById = async (id: string, type: string) => {
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
