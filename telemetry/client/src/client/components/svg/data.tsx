
export type d3Node = {
    id: string,
    group: number,
    name: string,
    radius: number,
    time?: number,
    dir?: string
};

export type d3Link = {
    source: string,
    target: string,
    value: number,
    isSelected?: boolean
};
  
export type Graph = {
    nodes: d3Node[],
    links: d3Link[]
};