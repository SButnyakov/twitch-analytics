export type TimepointsGraphProps = {
    id: TimepointsGraphId
    days: TimepointsGraphDays
    type: TimepointsGraphType
}
export type TimepointsGraphId = number;
export type TimepointsGraphDays = number;
export type TimepointsGraphType = string;

export type TimepointsGraphResponse = {
    data: TimepoinsGraphElement[]
}

export type TimepoinsGraphElement = {
    online: TimepointsGraphResponseOnline
    timestamp: TimepointsGraphResponseTimestamp
}
export type TimepointsGraphResponseOnline = number;
export type TimepointsGraphResponseTimestamp = string;
