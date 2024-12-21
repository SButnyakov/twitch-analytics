import SearchEngine from "../components/SearchEngine/SearchEngine";
import UIInput from "../ui/UIInput/UIInput";
import UIPage from "../ui/UIPage/UIPage";

const SearchPage: React.FC = () => {
    return (
        <UIPage title="SearchPage">
            <SearchEngine inputPlaceholder="Diablo IV" resultsLimit={5} />
        </UIPage>
    )
}

export default SearchPage;
