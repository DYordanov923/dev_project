import pickle

path = "/home/ubuntu/environments/project_files/ingest-wrong-resolution.pickle"

pickle_in = open(path,'rb')
example_dict = pickle.load(pickle_in)

print(example_dict)
