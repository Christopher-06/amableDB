<p align="center">
<img src="https://raw.githubusercontent.com/Christopher-06/christopher-06/master/amabledb-logo.jpg" width="75%"/>

**AmableDB** is a fast nosql database to find similar float-vectors in big datasets. For that purpose, it depends on the [NMSLIB](https://github.com/nmslib/hnswlib) library. All CRUD-Operations are supported via easy HTTP-Requests (GET AND POST) so all programming language can use this project. Just pull the Docker-Image and start quering. You can find a documentation in the [Wiki](https://github.com/Christopher-06/amableDB/wiki/Getting-Started).
</p>

<br />
<br />

# Motivation
For my project [Hive Discover](https://github.com/hive-discover) I had to do some KNN-Search over Post-Data. A neural network analyzed the text-body of different posts. As a result, I got 46 different values from 0 - 1 which represents the 46 categories. So to find similar posts I had to find similar vectors and I looked for a library for this kind of job. I ended up using the NMSLIB-Implementation of k-Nearest-Neighbor. I found out that elastic-search supports search operations but it required way to much memory. So I thought: Can this not be lightweight? Also elastic-search does not support directly the knn-Search.

That is why I created the amableDB project. It should help people who want to find quick and easy similar vectors with different search algorithms (currently only the knn-Search is available; later will be more). Also machine learning algorithms like SVC or Linear Regression are planned which should be easy to perform on collections.

<br />
<br />

# Getting Started
Simply run these two commands and you got your first node running:

- ` [1] docker pull christopher2002/amable-db:latest`
- ` [2] docker run -d --name amabledb -p 3399:3399 christopher2002/amable-db:latest`

For more information visit the [Docs](https://github.com/Christopher-06/amableDB/wiki/Getting-Started)