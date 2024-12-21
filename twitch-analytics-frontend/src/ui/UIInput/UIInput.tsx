type UIInputProps = {
    type?: string;
    placeholder?: string;
    value?: string;
    onInput?: React.ChangeEventHandler<HTMLInputElement>;
}

const UIInput: React.FC<UIInputProps> = ({type = 'text', placeholder = '', value, onInput}) => {
    return (
        <input type={type} placeholder={placeholder} value={value} onInput={onInput}/>
    )
}

export default UIInput;
