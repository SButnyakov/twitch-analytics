import { useParams } from "react-router-dom";
import UIPage from "../ui/UIPage/UIPage";
import TimepointsGraph from "../components/TimepointsGraph.tsx/TimepointsGraph";
import StreamerStats from "../components/StreamerStats/StreamerStats";

const StreamerPage: React.FC = () => {
    const { sid } = useParams();

    if (sid === undefined) {
        return (
            <UIPage title='Стример'>
                <p>Анлак</p>
            </UIPage>
        )
    }

    return (
        <UIPage title='Стример'>
            <StreamerStats id={parseInt(sid)}></StreamerStats>
            <TimepointsGraph id={parseInt(sid)}  days={1} type='streamer'></TimepointsGraph>
            <TimepointsGraph id={parseInt(sid)}  days={7} type='streamer'></TimepointsGraph>
            <TimepointsGraph id={parseInt(sid)}  days={30} type='streamer'></TimepointsGraph>
        </UIPage>
    )
};

export default StreamerPage;
