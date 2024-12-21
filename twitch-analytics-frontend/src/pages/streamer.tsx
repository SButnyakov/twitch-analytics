import { useParams } from "react-router-dom";
import UIPage from "../ui/UIPage/UIPage";

const StreamerPage: React.FC = () => {
    const { id } = useParams();

    return (
        <UIPage title='Стример'>
            <h1>Стример с id {id}</h1>
        </UIPage>
    )
};

export default StreamerPage;
