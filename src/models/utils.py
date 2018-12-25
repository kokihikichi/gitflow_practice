from ..imports import *

# Default word tokens
global PAD_TOKEN, SOS_TOKEN, EOS_TOKEN
PAD_TOKEN = 0  # Used for padding short sentences
SOS_TOKEN = 1  # Start-of-sentence token
EOS_TOKEN = 2  # End-of-sentence token

class Voc:
    def __init__(self):
        self.pad_token = PAD_TOKEN
        self.sos_token = SOS_TOKEN
        self.eos_token = EOS_TOKEN
        self.word2index = {}
        self.word2count = {}
        self.index2word = {self.pad_token: "PAD", self.sos_token: "SOS", self.eos_token: "EOS"}
        self.num_words = 3  # Count SOS, EOS, PAD

    def addSentence(self, sentence):
        for word in sentence.split(' '):
            self.addWord(word)

    def addWord(self, word):
        if word not in self.word2index:
            self.word2index[word] = self.num_words
            self.word2count[word] = 1
            self.index2word[self.num_words] = word
            self.num_words += 1
        else:
            self.word2count[word] += 1

def build_voc(data):
    """Input: data - [("word1 word2 word3", label), ("word1 word2 word3", label)]"""
    voc = Voc()
    for line in data: voc.addSentence(line[0])
    print("Counted words:", voc.num_words)
    return voc

def prepare_batch(data, voc, batch_size, max_len, pad_token=PAD_TOKEN, volatile=False):
    """[Archived] Prepare data for training / evaluation by randomly sampling batches."""
    sents, tgts = tuple(zip(*random.sample(data, k=batch_size)))
    sents = list(map(lambda x: list(map(lambda y: voc.word2index[y], x.split())), sents))
    for idx, x in enumerate(sents): sents[idx] = [pad_token]*(max_len - len(x)) + sents[idx]
    return Variable(torch.LongTensor(sents).t(), volatile=volatile), \
           Variable(torch.LongTensor(tgts), volatile=volatile)

def prepare_train_batch(data, voc, st_idx, batch_size, max_len, pad_token=PAD_TOKEN, volatile=False):
    """Prepare data for training / evaluation by iteratively slicing data."""
    sents, tgts = tuple(zip(*data[st_idx: st_idx+batch_size]))
    sents = list(map(lambda x: list(map(lambda y: voc.word2index[y], x.split())), sents))
    for idx, x in enumerate(sents): sents[idx] = [pad_token]*(max_len - len(x)) + sents[idx]
    return Variable(torch.LongTensor(sents).t(), volatile=volatile), \
           Variable(torch.LongTensor(tgts), volatile=volatile)

def prepare_pred_batch(data, voc, st_idx, batch_size, max_len, pad_token=PAD_TOKEN, volatile=False):
    """Prepare data for training / evaluation by iteratively slicing data."""
    sents, tgts = tuple(zip(*data[st_idx: st_idx+batch_size]))
    sents = list(map(lambda x: list(map(lambda y: voc.word2index[y], x.split())), sents))
    for idx, x in enumerate(sents): sents[idx] = [pad_token]*(max_len - len(x)) + sents[idx]
    return torch.LongTensor(sents).t(), torch.LongTensor(tgts)

def acc(y,yh): return torch.mean((y==yh).type(torch.FloatTensor)).item()

def Frobenius(mat): return torch.norm(mat)

def trim_data(data, frac):
    new_data = []
    old_len = 0
    new_len = 0
    for x, y in data:
        words = x.split()
        old_len += len(words)
        split_idx = int(frac*len(words))
        if split_idx%2 == 0: words = words[:split_idx]
        else: words = words[:split_idx-1]
        new_len += len(words)
        new_data.append((" ".join(words), y))
    print("Average length of sequences changed from {:.1f} to {:.1f}".format(old_len/len(data), new_len/len(data)))
    return new_data