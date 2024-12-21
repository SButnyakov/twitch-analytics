import { SearchEngineSuggestionListItemProps } from "../../types/searchEngine";

const SearchEngineSuggestionListItem: React.FC<SearchEngineSuggestionListItemProps> = ({ item, prefix }) => {
    const href = `/${prefix}/${item.id}`
    return (
        <a href={href} title={item.name}>{item.name}</a>
    )
};

export default SearchEngineSuggestionListItem;
