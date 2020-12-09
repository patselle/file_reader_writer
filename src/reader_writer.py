import os
import sys
import time
import glob
import _thread

import reader_writer_utils as utils

class job():
	def __init__(self, data, output_path, processed_path):
		self.data = data
		self.output_path = output_path
		self.processed_path = processed_path


class reader_writer():
	def __init__(
		self,
		process,
		input_root,
		output_root,
		show_progress,
		thread_limit,
		max_cpu_count,
		max_queue_size,
		clean_working_dir,
		batch_size,
		processed_root):

			self.__input_root = input_root
			self.__output_root = output_root
			self.__processed_root = processed_root
			self.__show_progress = show_progress
			self.__thread_limit = thread_limit
			self.__max_cpu_count = max_cpu_count
			self.__max_queue_size = max_queue_size
			self.__clean_working_dir = clean_working_dir
			self.__batch_size = batch_size

			self.__process = process
			self.__all_files_count = 0
			
			self.__processed = 0
			self.__processed_estimation = 0
			self.__eta = None
			self.__last_print = 0
			self.__pending = 0
			self.__ready = []
			self.__finished = []
			self.__spawns = 0
			self.__lock = _thread.allocate_lock()
		


	def __append_zeros(self, val, length):
		if len(val) == 2:
			val += '.'

		while len(val) < length:
			val += '0'

		return val


	def __get_eta(self, alpha):
		if not self.__eta:
			return self.__processed_estimation
		return alpha * self.__processed_estimation + (1 - alpha) * self.__eta


	def __estimate_time(self):

		self.__eta = self.__get_eta(alpha=0.5)
		minutes, seconds = divmod(self.__eta * (self.__all_files_count - self.__processed) / 10., 60) 
		hours, minutes = divmod(minutes, 60)

		return "%02d:%02d:%02d" % (hours, minutes, seconds)


	def __percentage(self):
		return int(float(self.__processed) / self.__all_files_count * 10000) / 100.0



	def __print_progress(self):
		now = time.time()

		if now - self.__last_print < 10:
			return

		to_print = f'{self.__append_zeros(str(self.__percentage()), 5)}% '
		to_print += f'({self.__processed}/{self.__all_files_count}) '
		to_print += f'eta: {self.__estimate_time()} '
		to_print += f'pending: {self.__pending} '
		to_print += f'threads: {self.__spawns}'
		print(to_print)

		self.__processed_estimation = 0
		self.__last_print = now


	def __print_information(self):
		to_print = '### Job information\n'
		to_print += '-------------------------------------------\n'
		to_print += f'input dir: {self.__input_root}\n'
		to_print += f'output dir: {self.__output_root}\n'
		to_print += f'process dir: {self.__processed_root}\n'
		to_print += f'max cpu: {self.__max_cpu_count}\n'
		to_print += '-------------------------------------------\n'
		print(to_print)

	
	def __processor_loop(self, name):
		while True:
			job = None
		
			self.__lock.acquire()
			if len(self.__ready) > 0:
				job = self.__ready.pop(0)
			self.__lock.release()

			if job is None:
				break

			job.result = self.__process(job.data)

			self.__lock.acquire()
			self.__finished.append(job)
			self.__lock.release()

		self.__lock.acquire()
		self.__spawns -= 1
		print(f'Exit thread [{self.__spawns}]')
		self.__lock.release()


	def __create_processor(self):
		spawn = None

		self.__lock.acquire()
		# max thread should be the same as physical/logical processor count
		if self.__spawns < self.__max_cpu_count:
			spawn = self.__spawns
			self.__spawns += 1
		self.__lock.release()

		if spawn is not None:
			print(f'Start new thread [{spawn}]')
			_thread.start_new_thread(self.__processor_loop, (spawn,))


	def __write_all_finished(self):
		count = 0

		while True:
			job = None

			self.__lock.acquire()
			if len(self.__finished) > 0:
				job = self.__finished.pop(0)
			self.__lock.release()

			if job is None:
				break

			utils.ensure_directory(os.path.dirname(job.output_path))
			utils.ensure_directory(os.path.dirname(job.processed_path))

			with open(job.output_path, 'w') as f:
				print(job.result.getvalue(), file=f)

			open(job.processed_path, 'a').close()

			count += 1

		if count == 0:
			return 

		self.__processed += count
		self.__processed_estimation += count
		self.__pending -= count

		if self.__show_progress:
			self.__print_progress()


	def __append_job(self, data, output_path, processed_path):
		self.__lock.acquire()
		self.__ready.append(job(data, output_path, processed_path))
		create_processor = len(self.__ready) > self.__thread_limit or self.__spawns == 0
		self.__lock.release()
		return create_processor


	def run(self):
		if self.__show_progress:
			self.__all_files_count = sum(1 for _ in utils.iterate_files(self.__input_root))
			if self.__all_files_count == 0:
				print('No files found')
				sys.exit()
			self.__print_information()

		utils.ensure_directory(self.__processed_root)
		
		for f in utils.iterate_files(self.__input_root):
			
			input_path = os.path.join(self.__input_root, f)
			output_path = os.path.join(self.__output_root, f)
			processed_path = os.path.join(self.__processed_root, f'{f}.processed')

			while True:
				self.__write_all_finished()

				if os.path.isfile(processed_path):
					self.__processed += 1
					break
				
				if self.__pending < self.__max_queue_size:
					with open(input_path, 'r') as fs:
						create_processor = self.__append_job(fs.read(), output_path, processed_path)
						self.__pending += 1

						if create_processor:
							self.__create_processor()		
						break
				else:
					time.sleep(1)
					continue		

		while self.__pending > 0:
			time.sleep(1)
			self.__write_all_finished()

		if self.__clean_working_dir:
			utils.clean_dir(self.__processed_root)
			
