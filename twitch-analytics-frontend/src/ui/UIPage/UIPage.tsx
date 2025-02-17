import React, { ReactNode } from "react";
import "../../css/UIPage.css"
import { useSetTitle } from "../../hooks/useSetTitle";

type UIPageProps = {
    title: string;
    children: ReactNode;
}

const UIPage: React.FC<UIPageProps> = (props) => {
    useSetTitle(props.title)
    return (
        <div className="uipage">
            <main>{props.children}</main>
        </div>
    );
}

export default UIPage;
