import { useParams } from "react-router-dom";
import UIPage from "../ui/UIPage/UIPage";
import TimepointsGraph from "../components/TimepointsGraph.tsx/TimepointsGraph";

const GamePage: React.FC = () => {
    let { gid } = useParams();

    if (gid === undefined) {
        return (
            <UIPage title='Game'>
                <p>Анлак</p>
            </UIPage>
        )
    }

    return (
        <UIPage title='Game'>
            <TimepointsGraph id={parseInt(gid)}  days={1} type='game'></TimepointsGraph>
            <TimepointsGraph id={parseInt(gid)}  days={7} type='game'></TimepointsGraph>
            <TimepointsGraph id={parseInt(gid)}  days={30} type='game'></TimepointsGraph>
        </UIPage>
    )
}

export default GamePage;
