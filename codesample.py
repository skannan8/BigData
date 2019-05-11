import numpy as np
from numpy import genfromtxt
import sys
import pandas as pd
import csv


file = 'tree.csv'

print("Reading input data")
input_data = list(csv.reader(open(file)))
print("Completed reading input data")

# converting json dataset from dictionary to dataframe
# Mapping of query to columns. Eg: Query 1 -> {2,3,4} columns
stratified_columns = [6,7,9]
stratified_K = dict()
qcs_dict = [dict() for x in range(len(stratified_columns))]
qcs_counts = [dict() for x in range(len(stratified_columns))]
# print("Input data is: ", (input_data))
num_rows = np.shape(input_data)[0]
num_columns = np.shape(input_data)[1]

# Do uniform sampling with given sampling factor, for each column in qcs
def uniform_sampling(sampling_factor):
    num_samples = int(num_rows * sampling_factor)
    indexes = np.round(np.random.uniform(low =0, high= num_rows -1 , size=num_samples))
    indexes= indexes.astype(int)
    return np.array(input_data)[indexes]

def uniform_sampling_wo_replacement(a, K, replace):
    # print("Input is ", a)
    sample = (np.random.choice(a, size=K, replace=replace))
    # print("Choice Sample size selelcted is: ", sample)
    return sample

# Do stratified sampling with minimum K rows for unique keys, for each column in qcs
def stratified_sampling(qc, K):
    if qc not in stratified_columns:
        return
    index = stratified_columns.index(qc)
    K = stratified_K[index]
    qcs = qcs_dict[index]
    stratified_sample = list()

    # print(qcs_dict)
    for key in qcs:
        if np.size(qcs[key]) < K:
            # print("All samples selected for key: ", key, "  size: ", np.size(qcs[key]))
            stratified_sample.extend(np.asarray(qcs[key])[0])
        else:
            sample = uniform_sampling_wo_replacement(np.asarray(qcs[key], dtype =int)[0], K, False)
            # print("Choice Sample received for key: " , key, "  is :", len(sample))
            stratified_sample.extend(sample)
    print("K value is: ", K)
    print("Unique keys: ", len(qcs.keys()))
    print("Sample size: ", len(stratified_sample))
    print("Unique selected samples: ", len(np.unique(stratified_sample)))
  
    return np.array(input_data)[stratified_sample]

def compute_stratified_K():
    i=0
    for index in stratified_columns:
        col = np.array(input_data)[:,index]
        print(col)
        unique_elements , counts= np.unique(col, return_counts= True)
        for unique_element in unique_elements:
            where_list = np.where(col == unique_element)
            qcs_dict[i][unique_element] = where_list
            qcs_counts[i][unique_element] = counts
        i+=1
    i=0
    for q_count in qcs_counts:
        values = q_count.values()
        stratified_K[i] = (int(np.mean(values)))
        # print("For column: ", stratified_columns[i], "K = ", np.round(np.mean(values)))
        i+=1
    return

# Do uniform sampling, specify sampling factor and columns
uniform_sample = uniform_sampling(0.8)
uniform_sample.reshape(int(0.8*num_rows), num_columns)
print("Uniform sample size  ", np.shape(uniform_sample))
print("Uniform sample size  ", type(uniform_sample))
r = np.shape(uniform_sample)[0]
c = np.shape(uniform_sample)[1]
df = pd.DataFrame(data=uniform_sample)
df.to_csv("tree_sample_0.8.csv", index=False, header=False)


# Do stratified sampling
# 1 time step, calculate K for each column to be stratified
compute_stratified_K()

# Do stratified sampling, specify columns used in the query
col = 9
K = 50000
stratified_sample = stratified_sampling(col, K)
df = pd.DataFrame(data=stratified_sample)
df.to_csv("tree_strat_sample_9_50K.csv", index=False, header=False)
print("Stratified sampling size: ", len(stratified_sample))
print(stratified_sample)

