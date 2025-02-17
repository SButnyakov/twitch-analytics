import { SearchEngineSuggestionListProps } from "../../types/searchEngine";
import "../../css/SearchEngineSuggestionList.css"
import SearchEngineSuggestionListItem from "./SearchEngineSuggestionListItem";

const SearchEngineSuggestionsList: React.FC<SearchEngineSuggestionListProps> = ({ isLoading, result, error }) => {
    if (error || !result) {
        return (
            <div>
                <h2>{error}</h2>
            </div>
        )
    }
    if (isLoading) {
        return (
            <div>
                <h2>Loading...</h2>
            </div>
        )
    }

    return (
        <div className={!result.games && !result.streamers ? "collapsed" : ""}>
            {result.games && result.games.length > 0 &&
            <div>
                <h2>Games</h2>
                <ul>
                   {result.games.map(game => (
                    <li key={game.id}><SearchEngineSuggestionListItem item={game} prefix={'game'}/></li>
                   ))}
                </ul>
            </div>
            }
            {result.streamers && result.streamers.length > 0 &&
            <div>
                <h2>Streamers</h2>
                <ul>
                   {result.streamers.map(streamer => (
                    <li key={streamer.id}><SearchEngineSuggestionListItem item={streamer} prefix={'streamer'}/></li>
                   ))}
                </ul>
            </div>
            }
        </div>
    )
}

export default SearchEngineSuggestionsList;
