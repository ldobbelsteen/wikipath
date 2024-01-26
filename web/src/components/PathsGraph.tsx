import {
  D3DragEvent,
  D3ZoomEvent,
  drag,
  forceCenter,
  forceLink,
  forceManyBody,
  forceSimulation,
  scaleOrdinal,
  schemeCategory10,
  select,
  zoom,
} from "d3";
import { SimulationLinkDatum, SimulationNodeDatum } from "d3-force";
import React, { useEffect, useRef } from "react";
import { Paths } from "../api";

type Link = SimulationLinkDatum<Node>;
interface Node extends SimulationNodeDatum {
  id: number;
  title: string;
  degree: number;
}

export const PathsGraph = (props: {
  paths?: Paths | "loading";
  className: string;
}) => {
  const ref = useRef<SVGSVGElement>(null);

  const { paths } = props;

  /** Re-render graph on data change */
  useEffect(() => {
    /** Clear any previous graph from the svg. */
    if (ref.current === null) return;
    const svg = select(ref.current);
    svg.selectAll("*").remove();

    if (paths === "loading" || paths === undefined || paths.count === 0) {
      return;
    }

    /** Extract nodes and links for D3 from the paths */
    const nodes: Node[] = [];
    const links: Link[] = [];
    paths.paths.forEach((path) => {
      let previousNode: Node;
      path.forEach((page, index) => {
        let currentNode = nodes.find((node) => node.id === page.id);
        if (!currentNode) {
          currentNode = {
            id: page.id,
            title: page.title,
            degree: index,
          };
          nodes.push(currentNode);
        }
        if (index != 0) {
          links.push({
            source: previousNode,
            target: currentNode,
          });
        }
        previousNode = currentNode;
      });
    });

    /** Force simulation; gravitate to center and gravitate away from eachother */
    const centerX = 0.5 * (ref?.current?.clientWidth || 0);
    const centerY = 0.5 * (ref?.current?.clientHeight || 0);
    const simulation = forceSimulation(nodes)
      .force("link", forceLink(links))
      .force("charge", forceManyBody().strength(-2000).distanceMax(300))
      .force("center", forceCenter(centerX, centerY));

    /** Add link arrow head definition to the svg */
    svg
      .append("svg:defs")
      .selectAll("marker")
      .data(["arrowhead"])
      .enter()
      .append("svg:marker")
      .attr("id", String)
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", 19)
      .attr("markerWidth", 6)
      .attr("markerHeight", 6)
      .attr("fill", "white")
      .attr("stroke", "white")
      .attr("orient", "auto")
      .append("svg:path")
      .attr("d", "M0,-5L10,0L0,5");

    /** Create group for the links */
    const link = svg
      .append("g")
      .attr("class", "links")
      .selectAll("line")
      .data(links)
      .enter()
      .append("line")
      .attr("stroke", "white")
      .attr("stroke-width", 1.5)
      .attr("marker-end", "url(#arrowhead)");

    /** Allow dragging nodes and their titles */
    const nodeDrag = drag<SVGGElement, Node>();
    nodeDrag.on(
      "start",
      (
        ev: D3DragEvent<SVGGElement, Node, Node | d3.SubjectPosition>,
        subject: Node,
      ) => {
        if (!ev.active) simulation.alphaTarget(0.3).restart();
        subject.fx = subject.x;
        subject.fy = subject.y;
      },
    );
    nodeDrag.on(
      "drag",
      (
        ev: D3DragEvent<SVGGElement, Node, Node | d3.SubjectPosition>,
        subject: Node,
      ) => {
        subject.fx = ev.x;
        subject.fy = ev.y;
      },
    );
    nodeDrag.on(
      "end",
      (
        ev: D3DragEvent<SVGGElement, Node, Node | d3.SubjectPosition>,
        subject: Node,
      ) => {
        if (!ev.active) simulation.alphaTarget(0);
        subject.fx = null;
        subject.fy = null;
      },
    );

    /** Add zoom and pan behaviour */
    svg.call(
      zoom<SVGSVGElement, unknown>()
        .on("zoom", (ev: D3ZoomEvent<SVGSVGElement, Node>) => {
          select(".pages").attr("transform", ev.transform.toString());
          select(".links").attr("transform", ev.transform.toString());
        })
        .scaleExtent([0.5, 4]),
    );

    /** Create group for the pages/nodes */
    const node = svg
      .append("g")
      .attr("class", "pages")
      .selectAll("g")
      .data(nodes)
      .enter()
      .append("g")
      .call(nodeDrag);

    /**
     * Make nodes clickable, opening a tab to the corresponding Wikipedia
     * article
     */
    const clickable = node
      .append("a")
      .attr("target", "_blank")
      .attr(
        "href",
        (node) =>
          `https://${paths.languageCode}.wikipedia.org/wiki/${node.title}`,
      );

    /** Represent the nodes as colored circles */
    const colors = scaleOrdinal(schemeCategory10);
    clickable
      .append("circle")
      .attr("r", 10)
      .attr("fill", (node) => colors(node.degree.toString()))
      .attr("stroke", "white")
      .attr("stroke-width", 2);

    /** Add the title corresponding to the node's page */
    clickable
      .append("text")
      .text((node) => {
        let text = node.title;
        if (
          (node.id === paths.source.id && paths.sourceIsRedirect) ||
          (node.id === paths.target.id && paths.targetIsRedirect)
        ) {
          text += " (redirected)";
        }
        return text;
      })
      .style("user-select", "none")
      .style("fill", "white")
      .attr("x", 16)
      .attr("y", 5);

    /** Start physics simulation */
    simulation.on("tick", () => {
      link
        .attr("x1", (node: Link) => (node.source as Node).x?.toString() || "")
        .attr("y1", (node: Link) => (node.source as Node).y?.toString() || "")
        .attr("x2", (node: Link) => (node.target as Node).x?.toString() || "")
        .attr("y2", (node: Link) => (node.target as Node).y?.toString() || "");
      node.attr("transform", (d) => `translate(${d.x || 0},${d.y || 0})`);
    });
  }, [paths]);

  return <svg className={props.className} ref={ref} />;
};
