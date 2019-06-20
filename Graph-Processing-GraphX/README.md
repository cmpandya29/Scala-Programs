# A Graph Processing Program using GraphX

This program finds the connected components of any undirected graph and prints the size of these connected components. A connected component of a graph is a subgraph of the graph in which there is a path from any two vertices in the subgraph. For example, there are two connected components: one 0,8,9 and another 1,2,3,4,5,6,7. Your program should print the sizes of these connected components: 3 and 7.

The following pseudo-code finds the connected components using Pregel:

Read the input graph and construct the RDD of edges
Use the graph builder Graph.fromEdges to construct a Graph from the RDD of edges
Access the VertexRDD and change the value of each vertex to be the vertex ID (initial group number)
Call the Graph.pregel method in the GraphX Pregel API to find the connected components. For each vertex, this method changes its group number to the minimum group number of its neighbors (if it is less than its current group number)
Group the graph vertices by their group number and print the group sizes

1. Run commands are mentioned in "graph.run" file.
