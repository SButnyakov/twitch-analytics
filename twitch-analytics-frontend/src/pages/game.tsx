import { useParams } from "react-router-dom";
import UIPage from "../ui/UIPage/UIPage";

const GamePage: React.FC = () => {
    const { id } = useParams();

    return (
        <UIPage title='Game'>
            <h1>Игра с id {id}</h1>
        </UIPage>
    )
}

export default GamePage;
