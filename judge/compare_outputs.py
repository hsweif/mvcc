import pandas as pd
import sys
from tqdm import tqdm
def compare(thread_num):
    for i in range(thread_num):
        print ('thread {}'.format(i+1))
        output_left = pd.read_csv('output_thread_'+str(i+1)+'.csv')
        output_right = pd.read_csv('real_thread_'+str(i+1)+'.csv')
        assert len(output_left) == len(output_right), '输出有遗漏'
        for idx in tqdm(range(len(output_left))):
            left_row = output_left.loc[idx]
            right_row = output_right.loc[idx]
            assert left_row['transaction_id'] == right_row['transaction_id'], str(left_row) + '\n' + str(right_row)
            assert left_row['type'] == right_row['type'], str(left_row) + '\n' + str(right_row)
            assert pd.isna(left_row['value']) and pd.isna(right_row['value']) or left_row['value'] == right_row['value'], str(left_row) + '\n' + str(right_row)

if __name__== "__main__":
    compare(int(sys.argv[1]))
