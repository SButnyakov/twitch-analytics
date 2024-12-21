import axios from 'axios';
import { config } from '../config/config';

const apiClient = axios.create({
    baseURL: config.BASE_API_HOST,
    timeout: config.API_TIMEOUT,
});

export const fetchGamesAndStreamers = async (searchString: string, top: number) => {

};
