from mpi4py import MPI
import time

def is_perfect(n):
    if n < 2:
        return False
    divisors_sum = sum([i for i in range(1, n//2 + 1) if n % i == 0])
    return divisors_sum == n

def read_input_file(file_name):
    with open(file_name, 'r') as f:
        numbers = [int(line.strip()) for line in f.readlines()]
    return numbers

def write_output_file(file_name, perfect_numbers):
    with open(file_name, 'w') as f:
        for number in perfect_numbers:
            f.write(f"{number}\n")

def distribute_numbers(numbers, size):
    """Розподілення чисел поміж процесів"""
    chunk_size = len(numbers) // size
    chunks = [numbers[i * chunk_size : (i + 1) * chunk_size] for i in range(size)]
    # Залишок чисел
    remainder = len(numbers) % size
    if remainder:
        for i in range(remainder):
            chunks[i].append(numbers[-(remainder-i)])
    return chunks

def main(input_file, output_file):
    start_time = time.time()

    comm = MPI.COMM_WORLD  # Об'єкт, що представляє групу процесів які можуть комунікувати один з одним
    size = comm.Get_size() # Загальна кількість процесів, які приймають участь в MPI програмі.
    rank = comm.Get_rank() # Унікальний ідентифікатор, який надається кожному процесу комунікатора

    numbers_to_check = None
    if rank == 0: # Якщо процес кореневий
        numbers = read_input_file(input_file)
        chunks = distribute_numbers(numbers, size)
    else:
        chunks = None

    # Розподілення чисел поміж всіх процесів
    numbers_to_check = comm.scatter(chunks, root=0)

    # Знаходження досконалих чисел в кожному з наборів чисел
    local_perfect_numbers = [n for n in numbers_to_check if is_perfect(n)]

    # Об'єднання всіх результатів в кореневому процесі
    all_perfect_numbers = comm.gather(local_perfect_numbers, root=0)

    if rank == 0:
        # Об'єднання списку а запис в файл
        all_perfect_numbers = [num for sublist in all_perfect_numbers for num in sublist]
        write_output_file(output_file, all_perfect_numbers)
        print("Perfect numbers found and written to the output file.")
        end_time = time.time()
        total_time_ms = (end_time - start_time) * 1000
        print(f"Total execution time: {total_time_ms:.2f} ms")

if __name__ == "__main__":
    input_file = "C:\\Users\\NITRO5\\PycharmProjects\\pythonProject2\\Lab3_MPI\\input_mpi.txt"
    output_file = "C:\\Users\\NITRO5\\PycharmProjects\\pythonProject2\\Lab3_MPI\\output_mpi.txt"
    main(input_file, output_file)
