export type SearchEngineProps = {
    inputType?: SearchEngineInputType
    inputPlaceholder?: SearchEngineInputPlaceholder
    resultsLimit: SearchEngineResultsLimit
};
export type SearchEngineResultsLimit = number;
export type SearchEngineSearchString = string;

export type SearchEngineInputProps = {
    type: SearchEngineInputType;
    placeholder: SearchEngineInputPlaceholder;
    value: SearchEngineInputValue;
    setValue: SearchEngineInputSetValue;
};
export type SearchEngineInputType = string;
export type SearchEngineInputValue = string;
export type SearchEngineInputPlaceholder = string;
export type SearchEngineInputSetValue = React.Dispatch<React.SetStateAction<string>>;

export type SearchEngineSuggestionListProps = {
    isLoading: SearchEnigneSuggestionListIsLoading;
    result: {
        streamers: SearchEngineSuggestionListArrayItem[];
        games: SearchEngineSuggestionListArrayItem[];
    } | null;
    error: SearchEngineSuggestionListError | null;
};
export type SearchEnigneSuggestionListIsLoading = boolean;
export type SearchEngineSuggestionListArrayItem = {
    id: SearchEngineSuggestionListArrayItemId;
    name: SearchEngineSuggestionListArrayItemName;
};
export type SearchEngineSuggestionListArrayItemId = string;
export type SearchEngineSuggestionListArrayItemName = string;
export type SearchEngineSuggestionListError = string;

export type SearchEngineSuggestionListItemProps = {
    item: SearchEngineSuggestionListArrayItem;
    prefix: SearchEngineSuggestionListItemEntityPrefix;
};
export type SearchEngineSuggestionListItemEntityPrefix = string;