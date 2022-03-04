# imports
import argparse
import pandas as pd


# return the index of the row, which has < cpu_cores tasks
def return_index(df_res, cpu_cores, group):
    if df_res.shape[0] == 0:
        return 0
    else:
        for ind, r in df_res.iterrows():
            if (len(r['task_name'].split(',')) < cpu_cores) & (group is None or r['group_name'] == group):
                return ind
    return df_res.shape[0]


# append to resulting dataframe
def append_rows(ind, df, r):
    for y in range(1, r['exec_time'] + 1):
        set_index = ind + y - 1
        if set_index < df.shape[0]:
            df.at[ind + y - 1, 'task_name'] = df.at[ind + y - 1, 'task_name'] + ',' + r['task_name']
        else:
            new_row = pd.DataFrame(
                {'time': y + ind, 'task_name': r['task_name'], 'group_name': r['group_name']},
                columns=['time', 'task_name', 'group_name'], index=[set_index])
            df = pd.concat([df, new_row], axis=0)
    return df


# Get the arguments from command line
# Create the parser
parser = argparse.ArgumentParser()
# Add the arguments
parser.add_argument('--cpu_cores', type=int, required=True)
parser.add_argument('--pipeline', type=str, required=True)
# Parse the argument
args = vars(parser.parse_args())

# read the file and store it in df
values_array = pd.read_csv(args['pipeline'], header=None, sep='\t', skip_blank_lines=False, skipfooter=1, engine='python').values.reshape(-1, 4)
df_data = pd.DataFrame(values_array, columns=['task_name', 'exec_time', 'group_name', 'dependents'])
df_data['exec_time'] = df_data['exec_time'].astype('int')
# sort dataframe by exec time desc
df_sorted = df_data.sort_values(by=['exec_time'], ascending=False)
df_result = pd.DataFrame([], columns=['time', 'task_name', 'group_name'])
# create a list with all already included tasks
task_list = []
# keep checking while the sorted list is emptied
while df_sorted.shape[0] > 0:
    # for each row of the sorted list
    for index, row in df_sorted.iterrows():
        # check if the task has dependents
        if row['dependents'] is not None:
            # check if the dependents are in the new dataframe
            if list(set(row['dependents'].split(',')) - set(task_list)):
                continue
            # if all dependents are in the new dataframe, add the task
            else:
                # get the index to write the new task
                index_to_write = return_index(df_result, args['cpu_cores'], row['group_name'])
                # append the rows of the task to the result at the specific index
                df_result = append_rows(index_to_write, df_result, row)
                # add the task to the already processed task list
                task_list.append(row['task_name'])
                # remove the task from the sorted list
                df_sorted = df_sorted[df_sorted['task_name'] != row['task_name']]
                # create a dataframe, which holds all other tasks with the same group
                df_processed = df_sorted[df_sorted['group_name'].isin([row['group_name']])]
                break
        # if there are no dependents
        else:
            # get the index to write the new task
            index_to_write = return_index(df_result, args['cpu_cores'], row['group_name'])
            # append the rows of the task to the result at the specific index
            df_result = append_rows(index_to_write, df_result, row)
            # add the task to the already processed task list
            task_list.append(row['task_name'])
            # remove the task from the sorted list
            df_sorted = df_sorted[df_sorted['task_name'] != row['task_name']]
            # create a dataframe, which holds all other tasks with the same group
            df_processed = df_sorted[df_sorted['group_name'].isin([row['group_name']])]
            break
    # while there are rows in the dataframe with tasks with the same group
    while df_processed.shape[0] > 0:
        # for each row
        for index, row in df_processed.iterrows():
            # check if the task has dependents
            if row['dependents'] is not None:
                # check if the dependents are in the new dataframe
                if list(set(row['dependents'].split(',')) - set(task_list)):
                    continue
                # if the dependents are in the result
                else:
                    # get the index to write the new task
                    index_to_write = return_index(df_result, args['cpu_cores'], row['group_name'])
                    # append the rows of the task to the result at the specific index
                    df_result = append_rows(index_to_write, df_result, row)
                    # add the task to the already processed task list
                    task_list.append(row['task_name'])
                    # remove the task from the sorted list
                    df_sorted = df_sorted[df_sorted['task_name'] != row['task_name']]
                    # remove the task from the processing
                    df_processed = df_processed[df_processed['task_name'] != row['task_name']]
            # if there are no dependents
            else:
                # get the index to write the new task
                index_to_write = return_index(df_result, args['cpu_cores'], row['group_name'])
                # append the rows of the task to the result at the specific index
                df_result = append_rows(index_to_write, df_result, row)
                # add the task to the already processed task list
                task_list.append(row['task_name'])
                # remove the task from the sorted list
                df_sorted = df_sorted[df_sorted['task_name'] != row['task_name']]
                # remove the task from the processing
                df_processed = df_processed[df_processed['task_name'] != row['task_name']]

print(f'Minimum Execution Time = {df_result.time.max()} minute')
print(df_result)
df_result.to_csv('result.csv')










