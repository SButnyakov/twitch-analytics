import { SearchEngineInputProps, SearchEngineInputPlaceholder } from "../../types/searchEngine";
import UIInput from "../../ui/UIInput/UIInput";

const SearchEngineInput: React.FC<SearchEngineInputProps> = ({type, placeholder, value, setValue}) => {
    const handleInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        setValue(event.target.value)
    };

    return (
        <UIInput type={type} placeholder={placeholder} value={value} onInput={handleInputChange}/>
    )
};

export default SearchEngineInput;
