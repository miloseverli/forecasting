import { Plot, PlotData, plot } from "nodeplotlib";
import fs from "fs";

let data = fs.readFileSync("data/test-data.csv", "utf-8").split("\n");

let areas = {} as { [key: string]: { y: number[]; x: string[] } };

for (let line of data) {
    let splitted = line.split(",");
    let area = splitted[1];
    let date = splitted[2];
    let value = splitted[3];
    areas[area] = areas[area] ? areas[area] : { x: [], y: [] };
    areas[area].y.push(parseInt(value));
    areas[area].x.push(date);
}
let plotData = [] as Plot[];

let predictions = fs.readFileSync("out/joined.csv", "utf-8").split("\n");

for (let line of predictions) {
    let splitted = line.split(",");
    let area = splitted[0];
    let date = splitted[1].substring(0, 10);
    let value = splitted[3];
    areas[area] = areas[area] ? areas[area] : { x: [], y: [] };
    areas[area].y.push(parseInt(value));
    areas[area].x.push(date);
}

for (let value in areas) {
    plotData.push({
        name: value,
        x: areas[value].x,
        y: areas[value].y,
        type: "scatter",
    });
}

plot(
    plotData.filter((a) => ["SV1","sv1"].includes(a.name!)),
    { width: 800, height: 600 },
    {}
);
