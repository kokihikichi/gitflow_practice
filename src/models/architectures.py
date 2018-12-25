from ..imports import *

class BiLSTM(nn.Module):
    def __init__(self, conf):
        super(BiLSTM, self).__init__()
        self.conf = conf
        self.drop = nn.Dropout(self.conf["dropout"])
        self.encoder = nn.Embedding(self.conf["voc"].num_words, self.conf["emb_size"])
        self.bilstm = nn.LSTM(self.conf["emb_size"], self.conf["hid_size"], self.conf["n_lstm_layers"], 
                              dropout=self.conf["dropout"], bidirectional=True)
        self.init_weights()
        self.encoder.weight.data[self.conf["pad_token"]] = 0

    # note: init_range constraints the value of initial weights
    def init_weights(self, init_range=0.1):
        self.encoder.weight.data.uniform_(-init_range, init_range)

    def forward(self, inp, hidden):
        emb = self.drop(self.encoder(inp))
        outp = self.bilstm(emb, hidden)[0]
        if self.conf["pooling"] == 'mean': outp = torch.mean(outp, 0).squeeze()
        elif self.conf["pooling"] == 'max': outp = torch.max(outp, 0)[0].squeeze()
        elif self.conf["pooling"] == 'all' or self.pooling == 'all-word': 
            outp = torch.transpose(outp, 0, 1).contiguous()
        return outp, emb

    def init_hidden(self, bsz):
        return torch.zeros((self.conf["n_lstm_layers"] * 2, bsz, self.conf["hid_size"])), torch.zeros((self.conf["n_lstm_layers"] * 2, bsz, self.conf["hid_size"]))
#         weight = next(self.parameters()).data
#         return (Variable(weight.new(self.conf["n_lstm_layers"] * 2, bsz, self.conf["hid_size"]).zero_()),
#                 Variable(weight.new(self.conf["n_lstm_layers"] * 2, bsz, self.conf["hid_size"]).zero_()))


class SelfAttentiveEncoder(nn.Module):
    def __init__(self, conf):
        super(SelfAttentiveEncoder, self).__init__()
        self.conf = conf
        self.bilstm = BiLSTM(conf)
        self.drop = nn.Dropout(self.conf["dropout"])
        self.ws1 = nn.Linear(self.conf["hid_size"] * 2, self.conf["attn_size"], bias=False)
        self.ws2 = nn.Linear(self.conf["attn_size"], self.conf["attn_hops"], bias=False)
        self.tanh = nn.Tanh()
        self.softmax = nn.Softmax(dim=-1)
        self.init_weights()

    def init_weights(self, init_range=0.1):
        self.ws1.weight.data.uniform_(-init_range, init_range)
        self.ws2.weight.data.uniform_(-init_range, init_range)

    def forward(self, inp, hidden):
        outp = self.bilstm.forward(inp, hidden)[0]
        size = outp.size()  # [bsz, len, nhid]
        compressed_embeddings = outp.view(-1, size[2])  # [bsz*len, nhid*2]
        transformed_inp = torch.transpose(inp, 0, 1).contiguous()  # [bsz, len]
        transformed_inp = transformed_inp.view(size[0], 1, size[1])  # [bsz, 1, len]
        concatenated_inp = [transformed_inp for i in range(self.conf["attn_hops"])]
        concatenated_inp = torch.cat(concatenated_inp, 1)  # [bsz, hop, len]

        hbar = self.tanh(self.ws1(self.drop(compressed_embeddings)))  # [bsz*len, attention-unit]
        alphas = self.ws2(hbar).view(size[0], size[1], -1)  # [bsz, len, hop]
        alphas = torch.transpose(alphas, 1, 2).contiguous()  # [bsz, hop, len]
        penalized_alphas = alphas + (
            -10000 * (concatenated_inp == self.conf["pad_token"]).float())
            # [bsz, hop, len] + [bsz, hop, len]
        alphas = self.softmax(penalized_alphas.view(-1, size[1]))  # [bsz*hop, len]
        alphas = alphas.view(size[0], self.conf["attn_hops"], size[1])  # [bsz, hop, len]
        return torch.bmm(alphas, outp), alphas

    def init_hidden(self, bsz):
        return self.bilstm.init_hidden(bsz)


class Classifier(nn.Module):
    def __init__(self, conf):
        super(Classifier, self).__init__()
        self.conf = conf
        self.encoder = SelfAttentiveEncoder(conf)
        self.fc = nn.Linear(self.conf["hid_size"] * 2 * self.conf["attn_hops"], self.conf["fc_size"])
        self.drop = nn.Dropout(self.conf["dropout"])
        self.tanh = nn.Tanh()
        self.pred = nn.Linear(self.conf["fc_size"], self.conf["fc_size2"])
        self.pred2 = nn.Linear(self.conf["fc_size2"], self.conf["n_classes"])
        self.init_weights()

    def init_weights(self, init_range=0.1):
        self.fc.weight.data.uniform_(-init_range, init_range)
        self.fc.bias.data.fill_(0)
        self.pred.weight.data.uniform_(-init_range, init_range)
        self.pred.bias.data.fill_(0)

    def forward(self, inp, hidden):
        outp, attention = self.encoder.forward(inp, hidden)
        outp = outp.view(outp.size(0), -1)
        fc = self.tanh(self.fc(self.drop(outp)))
        pred = self.pred(self.drop(fc))
        pred = F.log_softmax(self.pred2(torch.relu(pred)), dim=1)
#         pred = self.pred2(torch.relu(pred))
        if type(self.encoder) == BiLSTM:
            attention = None
        return pred, attention

    def init_hidden(self, bsz):
        return self.encoder.init_hidden(bsz)

    def encode(self, inp, hidden):
        return self.encoder.forward(inp, hidden)[0]
    
class SimpleClassifier(nn.Module):
    def __init__(self, conf):
        super(SimpleClassifier, self).__init__()
        self.conf = conf
        self.encoder = BiLSTM(conf)
#         self.fc = nn.Linear(self.conf["hid_size"] * 2 * self.conf["attn_hops"], self.conf["fc_size"])
        self.fc = nn.Linear(self.conf["hid_size"] * 2, self.conf["fc_size"])
        self.drop = nn.Dropout(self.conf["dropout"])
        self.tanh = nn.Tanh()
        self.pred = nn.Linear(self.conf["fc_size"], self.conf["fc_size2"])
        self.pred2 = nn.Linear(self.conf["fc_size2"], self.conf["n_classes"])
        self.init_weights()

    def init_weights(self, init_range=0.1):
        self.fc.weight.data.uniform_(-init_range, init_range)
        self.fc.bias.data.fill_(0)
        self.pred.weight.data.uniform_(-init_range, init_range)
        self.pred.bias.data.fill_(0)
        self.pred2.weight.data.uniform_(-init_range, init_range)
        self.pred2.bias.data.fill_(0)

    def forward(self, inp, hidden):
        outp = self.encoder.forward(inp, hidden)[0]
        outp = torch.mean(outp, 0).squeeze()
        outp = outp.view(outp.size(0), -1)
        fc = self.tanh(self.fc(self.drop(outp)))
        pred = self.pred(self.drop(fc))
        pred = self.pred2(torch.relu(pred))
        if type(self.encoder) == BiLSTM:
            attention = None
        return pred, attention

    def init_hidden(self, bsz):
        return self.encoder.init_hidden(bsz)

    def encode(self, inp, hidden):
        return self.encoder.forward(inp, hidden)[0]