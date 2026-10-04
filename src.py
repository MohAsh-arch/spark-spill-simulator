import random
import resource
import time
import sys
import json
import psutil

budget = 100 * 1024 * 1024 # place holder

def gen(N):
    for i in range(N):
        yield {"category":f"category_{i}", "amount": random.randint(1,1000) }


def naive_accumulator(n):
    
    total = {}
    random.seed(42)
    
    for record in gen(n):
        
        category = record["category"]
        
        amount = record["amount"]
        
        total[category] = total.get(category,0) + amount

    return total


def spill_accumulator(n):
    total = {}
    file_count = 0
    random.seed(42)

    for record in gen(n):
        category = record["category"]
        amount = record["amount"]
        flag = category in total
        total[category] = total.get(category,0) + amount
        
        if not flag:
            process = psutil.Process()
            current_memory = process.memory_info().rss
            if current_memory > budget:
                with open(f"json_file_{file_count}.json","w") as f:
                    json.dump(total,f)
                file_count += 1
                total = {}


    return total , file_count


def merge_spilled(left_dict , file_count):
    for i in range(file_count):
        with open(f"json_file_{i}.json","r") as f:
         data = json.load(f)
        
        for key , value in data.items():
            left_dict[key] = left_dict.get(key, 0) + value

    return left_dict


#----------------experiment---------------#


start=time.perf_counter()
total , file_count = spill_accumulator(5_000_000)
total_spilled = merge_spilled(total, file_count)
end = time.perf_counter()
duration=end-start
print("------------Spill------------")
print(f"Resource : {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024} MB\nDuration: {duration} s\nfiles created : {file_count}")


start=time.perf_counter()
total_naive = naive_accumulator(5_000_000)
end = time.perf_counter()
duration=end-start
print("--------Naive---------")
print(f"Resource : {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024} MB\nDuration: {duration} s")


both_same = total_naive == total_spilled

print(f"are they both have the same content ? {both_same}")
