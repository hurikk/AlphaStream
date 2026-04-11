from src.alphastream.utils import utils

def run():
    # generates and updates the dataset of all stocks listed on the B3
    utils.gen_actions_dataset()
    
    
if __name__ == "__main__":
    run()