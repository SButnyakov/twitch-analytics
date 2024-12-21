import { useState } from "react";
import { SearchEngineProps } from "../../types/searchEngine";
import SearchEngineInput from "./SearchEngineInput";
import { useSearchEngine } from "../../hooks/useSearchEngine";
import SearchEngineSuggestionsList from "./SearchEngineSuggestionList";

const SearchEngine: React.FC<SearchEngineProps> = ({inputType = 'text', inputPlaceholder = 'Diablo IV', resultsLimit}) => {
    const [searchString, setSearchString] = useState('');

    const { result, isLoading, error } = useSearchEngine(searchString, resultsLimit);

    console.log(result)

    return (
        <div>
            <SearchEngineInput type={inputType} placeholder={inputPlaceholder} value={searchString} setValue={setSearchString}/>
            <SearchEngineSuggestionsList isLoading={isLoading} result={result} error={error} />
        </div>
    )
};

export default SearchEngine;
