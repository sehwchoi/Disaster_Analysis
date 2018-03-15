1. The flattened result of the distributions over topics
100_319_af_topics1.csv
- This file contains the topic distribution and their word distribution over 100 topics based on the post-disaster tweets

100_319_bf_topics1.csv
- This file contains the topic distribution and their word distribution over 100 topics based on the pre-disaster tweets

2. The result of topic distributions in bytes format.’bf’ and ‘af’ indicates before and after period respectively
Distribution/result_af.pkl
Distribution/result_bf.pkl

3. Cosine similarities over topics within and between the periods
Matrix/af_af_matrix.npy - similarities scores within the post-disaster period. It has 100(post period topics)*100(post period topics) matrix.
Matrix/af_bf_matrix.npy - similarities scores between the post-disaster and the pre-disaster periods.It has 100(post period topics)*100(pre period topics) matrix.

Matrix/bf_bf_matrix.npy - similarities scores within the pre-disaster period. It has 100(pre period topics)*100(pre period topics) matrix.
Matrix/bf_af_matrix.npy - similarities scores between the pre-disaster and the post-disaster periods. It has 100(pre period topics)*100(post period topics) matrix.