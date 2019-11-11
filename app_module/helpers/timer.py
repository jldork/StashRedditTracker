import time

class Timer:
    def __init__(self, title=""):
        self.start = None
        self.end = None
        self.title = title
        
    def __enter__(self):
        self.start = time.time()
        print("\n{}".format(self.title))
        print("-"*len(self.title))
        print("Start:",time.strftime('%l:%M%p %Z on %b %d, %Y'))
    
    def __exit__(self, type, value, traceback):
        self.end = time.time()
        print("End:",time.strftime('%l:%M%p %Z on %b %d, %Y'))
        print("Preprocessing Time: {} minutes".format(str((self.end-self.start)/60)))