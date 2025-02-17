import { useTimepointsGraph } from "../../hooks/useTimepointsGraph";
import { TimepointsGraphProps } from "../../types/timepointsGraph";
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

const TimepointsGraph: React.FC<TimepointsGraphProps> = ({id, days, type}) => {
    const {timepoints, isLoading, error } = useTimepointsGraph(id, days, type)
    
    if (timepoints === null) {
        return (
            <div>
                <p>Данные не найдены</p>
            </div>
        )
    }

    const formattedData = timepoints.data.map(({ timestamp, online }) => ({
        timestamp: new Date(timestamp).toLocaleTimeString(),
        online,
    }));

    return (
        <ResponsiveContainer width="100%" height={300}>
            <LineChart data={formattedData}>
                <XAxis dataKey="timestamp" tick={{ fontSize: 12 }} />
                <YAxis tick={{ fontSize: 12 }} />
                <Tooltip />
                <Line type="monotone" dataKey="online" stroke="#8884d8" strokeWidth={2} dot={false} />
            </LineChart>
        </ResponsiveContainer>
    );
};

export default TimepointsGraph;
