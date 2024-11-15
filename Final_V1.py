# Importing Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, broadcast
import numpy as np
from typing import List, Dict, Set, Tuple
from collections import defaultdict
from tqdm import tqdm
import time
import psutil
import logging
from datetime import datetime

# Set Up Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'simrank_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# SimRank Computer Class
class SimRankComputer:

    # Constructor
    def __init__(self, C: float = 0.8, max_iter: int = 5, epsilon: float = 1e-4, max_partition_size: int = 1000):
        self.C = C
        self.max_iter = max_iter
        self.epsilon = epsilon
        self.max_partition_size = max_partition_size
        self.similarities = defaultdict(float)
        
    # Get Memory Usage
    def get_memory_usage(self):
        process = psutil.Process()
        memory_info = process.memory_info()
        return f"Memory Usage: {memory_info.rss / 1024 / 1024:.2f} MB"

    # Create Sparse Adjacency Lists
    def create_sparse_adjacency_lists(self, edges_df):
        logger.info("Creating sparse adjacency lists...")
        logger.info(self.get_memory_usage())
        
        in_neighbors = defaultdict(set)
        out_neighbors = defaultdict(set)
        
        total_edges = edges_df.count()
        logger.info(f"Total edges to process: {total_edges}")
        
        edge_list = edges_df.collect()
        for edge in tqdm(edge_list, desc="Processing edges"):
            citing_id = edge['citing_paper_id']
            cited_id = edge['cited_paper_id']
            in_neighbors[cited_id].add(citing_id)
            out_neighbors[citing_id].add(cited_id)
        
        logger.info("Finished creating adjacency lists")
        logger.info(self.get_memory_usage())
        return dict(in_neighbors), dict(out_neighbors)

    # Partition Graph in Smaller Parts for Memory Efficiency 
    def partition_graph(self, nodes: Set[str], in_neighbors: Dict[str, Set[str]]):
        logger.info(f"Starting graph partitioning with max partition size: {self.max_partition_size}")
        logger.info(self.get_memory_usage())
        
        partitions = []
        remaining_nodes = nodes.copy()
        
        with tqdm(total=len(nodes), desc="Partitioning nodes") as pbar:
            while remaining_nodes:
                partition = set()
                if remaining_nodes:
                    frontier = {next(iter(remaining_nodes))}
                else:
                    break
                    
                while frontier and len(partition) < self.max_partition_size:
                    if not frontier:
                        break
                    node = frontier.pop()
                    if node in remaining_nodes:
                        partition.add(node)
                        remaining_nodes.remove(node)
                        pbar.update(1)
                        
                        neighbors = in_neighbors.get(node, set())
                        frontier.update(neighbors & remaining_nodes)
                
                if partition:
                    partitions.append(partition)
        
        logger.info(f"Created {len(partitions)} partitions")
        logger.info(self.get_memory_usage())
        return partitions

    # Compute Partition Similarity
    def compute_partition_similarity(self, partition: Set[str], all_papers: Set[str], in_neighbors: Dict[str, Set[str]]):
        
        for iter_num in range(self.max_iter):
            max_diff = 0
            new_similarities = defaultdict(float)
            
            for node1 in partition:
                new_similarities[(node1, node1)] = 1.0
                
                for node2 in all_papers:
                    if node1 >= node2:  
                        continue
                    
                    in1 = in_neighbors.get(node1, set())
                    in2 = in_neighbors.get(node2, set())
                    
                    if not in1 or not in2:
                        continue
                    
                    sim_sum = 0
                    for n1 in in1:
                        for n2 in in2:
                            sim_sum += self.similarities.get((min(n1, n2), max(n1, n2)), 0)
                    
                    new_sim = (self.C / (len(in1) * len(in2))) * sim_sum
                    new_similarities[(min(node1, node2), max(node1, node2))] = new_sim
                    
                    old_sim = self.similarities.get((min(node1, node2), max(node1, node2)), 0)
                    max_diff = max(max_diff, abs(new_sim - old_sim))
            
            self.similarities.update(new_similarities)
            
            if max_diff < self.epsilon:
                break

    # Compute SimRank
    def compute_simrank(self, nodes_df, edges_df):
        logger.info(f"Starting SimRank computation with C={self.C}, max_iter={self.max_iter}")
        logger.info(self.get_memory_usage())
        
        logger.info("Collecting unique paper IDs...")
        all_papers = set(row['paper_id'] for row in nodes_df.collect())
        logger.info(f"Total papers: {len(all_papers)}")
        
        in_neighbors, out_neighbors = self.create_sparse_adjacency_lists(edges_df)
        logger.info(f"Created adjacency lists. In-neighbors: {len(in_neighbors)}, Out-neighbors: {len(out_neighbors)}")
        
        partitions = self.partition_graph(all_papers, in_neighbors)
        
        for node in tqdm(all_papers, desc="Initializing similarities"):
            self.similarities[(node, node)] = 1.0
        
        for i, partition in tqdm(enumerate(partitions, 1), desc="Computing partition similarities", total=len(partitions)):
            self.compute_partition_similarity(partition, all_papers, in_neighbors)
        
        logger.info(self.get_memory_usage())
        return dict(self.similarities)

# Get Similar Papers
def get_similar_papers(similarities: Dict, query_id: str, top_k: int = 5):
    logger.info(f"Finding top {top_k} similar papers for {query_id}")
    
    query_similarities = []
    for (id1, id2), sim in similarities.items():
        if id1 == query_id:
            query_similarities.append((id2, sim))
        elif id2 == query_id:
            query_similarities.append((id1, sim))
    
    query_similarities.sort(key=lambda x: x[1], reverse=True)
    return query_similarities[:top_k]

# Run Memory Efficient SimRank
def run_memory_efficient_simrank(nodes_df, edges_df, C_values: List[float], query_nodes: List[str], top_k: int = 5):
    logger.info("Starting SimRank analysis")
    logger.info(f"C values: {C_values}")
    logger.info(f"Query nodes: {query_nodes}")
    
    results = {}
    
    for C in C_values:
        logger.info(f"\nRunning SimRank with C = {C}")
        
        simrank = SimRankComputer(C=C)
        similarities = simrank.compute_simrank(nodes_df, edges_df)
        
        C_results = {}
        for query_id in query_nodes:
            similar_papers = get_similar_papers(similarities, query_id, top_k)
            C_results[query_id] = similar_papers
        
        results[C] = C_results
    return results

# Main
if __name__ == "__main__":

    # Neo4j connection details
    url = "neo4j://localhost:7687"
    username = "neo4j"
    password = "Garv@neo4j10"
    dbname = "neo4j"
    
    # Spark session
    spark = (
        SparkSession.builder.appName("Neo4j-Integration")
        .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.12:5.0.3_for_spark_3")
        .config("spark.sql.shuffle.partitions", "400")
        .config("spark.memory.fraction", "0.8")
        .config("spark.sql.autoBroadcastJoinThreshold", "512m")
        .config("neo4j.url", url)
        .config("neo4j.authentication.basic.username", username)
        .config("neo4j.authentication.basic.password", password)
        .config("neo4j.database", dbname)
        .getOrCreate()
    )

    try:
        # Load data
        logger.info("Loading data from Neo4j...")
        
        papers_df = spark.read.format("org.neo4j.spark.DataSource") \
            .option("url", url) \
            .option("authentication.type", "basic") \
            .option("authentication.basic.username", username) \
            .option("authentication.basic.password", password) \
            .option("database", dbname) \
            .option("query", "MATCH (p:Paper) RETURN p.id AS paper_id") \
            .load()

        cites_df = spark.read.format("org.neo4j.spark.DataSource") \
            .option("url", url) \
            .option("authentication.type", "basic") \
            .option("authentication.basic.username", username) \
            .option("authentication.basic.password", password) \
            .option("database", dbname) \
            .option("query", """
                MATCH (p1:Paper)-[:CITES]->(p2:Paper)
                RETURN p1.id AS citing_paper_id, p2.id AS cited_paper_id
            """) \
            .load()

        logger.info("Data loaded successfully")
        logger.info(f"Number of papers: {papers_df.count()}")
        logger.info(f"Number of citations: {cites_df.count()}")

        # Run analysis
        C_values = [0.7, 0.8, 0.9]
        query_nodes = ['1556418098']

        # Sample the data
        sampled_papers_df_1 = papers_df.sample(withReplacement=False, fraction=0.1)
        sampled_paper_ids = [row['paper_id'] for row in sampled_papers_df_1.select('paper_id').collect()]
        specific_nodes = ['1556418098']
        sampled_paper_ids = list(set(sampled_paper_ids).union(set(specific_nodes)))
        sampled_cites_df_1 = cites_df.filter(
            (col("citing_paper_id").isin(sampled_paper_ids)) & (col("cited_paper_id").isin(sampled_paper_ids))
        )
        sampled_papers_df_1 = papers_df.filter(col("paper_id").isin(sampled_paper_ids))
        logger.info(f"Sampled Papers DataFrame 1 count: {sampled_papers_df_1.count()}")
        logger.info(f"Sampled Cites DataFrame 1 count: {sampled_cites_df_1.count()}")

        nodes_df = sampled_papers_df_1
        edges_df = sampled_cites_df_1

        results = run_memory_efficient_simrank(nodes_df, edges_df, C_values, query_nodes)

        # Save results to file
        with open("simrank_results_V3.txt", "w") as f:
            for C in C_values:
                f.write(f"\nResults for C = {C}\n")
                for query_id in query_nodes:
                    f.write(f"\nTop similar papers for query ID {query_id}:\n")
                    for paper_id, similarity in results[C][query_id]:
                        f.write(f"Paper ID: {paper_id}, Similarity: {similarity:.4f}\n")

        logger.info("Results saved to simrank_results_V3.txt")

    except Exception as e:
        logger.error(f"Error during SimRank computation: {str(e)}", exc_info=True)
    
    finally:
        spark.stop()
        logger.info("Spark session stopped")