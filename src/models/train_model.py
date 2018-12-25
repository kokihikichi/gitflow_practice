from ..imports import *
from .utils import *
from sklearn.metrics import roc_auc_score

def train(model, train_data, optimizer, iters, val_data=None, gpu=True):
    # initialize
    conf = model.conf
    start_time = time.time()
    n = len(train_data)
    total_loss = 0; total_pure_loss = 0; total_acc = 0
    I = Variable(torch.zeros(conf["batch_size"], conf["attn_hops"], conf["attn_hops"]))    
    for i in range(conf["batch_size"]):
        for j in range(conf["attn_hops"]):
            I.data[i][j][j] = 1

    # prepare validation data
    if val_data:
        with torch.no_grad():
            valX, valY = prepare_pred_batch(val_data, conf["voc"], 0, len(val_data), max_len=conf["max_length"])
            if gpu:
                valX = valX.cuda()
                valY = valY.cuda()

    criterion = nn.CrossEntropyLoss()

    try:
        for iter in range(iters):

            if not model.training: model.train()
            # prepare data
            st_idx = int((iter%(n//conf["batch_size"]))*conf["batch_size"])
            trainX, trainY = prepare_train_batch(train_data, conf["voc"], st_idx, conf["batch_size"], max_len=conf["max_length"])
            if trainY.shape[0] < conf["batch_size"]:
                continue
            if gpu:
                trainX = trainX.cuda()
                trainY = trainY.cuda()
            # forward pass
            hidden = model.init_hidden(trainX.size(1))
            output, attention = model.forward(trainX, hidden)
            # compute loss
            loss = criterion(output.view(trainX.size(1), -1), trainY)
            total_pure_loss += loss.data
            attentionT = torch.transpose(attention, 1, 2).contiguous()
            extra_loss = Frobenius(torch.bmm(attention, attentionT) - I[:attention.size(0)])
            loss += conf["penalization_coeff"] * extra_loss
            # backprop
            optimizer.zero_grad()
            loss.backward()
            # clip gradient
            torch.nn.utils.clip_grad_norm_(model.parameters(), conf["clip"])
            # update
            optimizer.step()

            # log
            total_loss += loss.data.item()
            total_acc += acc(trainY, torch.argmax(output.view(trainX.size(1), -1), dim=1))
            if iter % conf["log_interval"] == 0 and iter > 0:
                if val_data: 
                    with torch.no_grad():
                        val_loss, val_score = validate(valX, valY, model)
                else: val_loss, val_score = (0., 0.)
                elapsed = time.time() - start_time
                print("iter {:3d} - loss {:5.4f} | pure loss {:5.4f} | val loss {:5.4f} | acc {:.2%} | val acc {:.2%}".format(
                    iter, total_loss / conf["log_interval"], 
                    total_pure_loss / conf["log_interval"], val_loss, 
                    total_acc / conf["log_interval"], val_score))
                total_loss = 0; total_pure_loss = 0; total_acc = 0
            # free up cache
            torch.cuda.empty_cache()
    
    except KeyboardInterrupt: print("\nTraining terminated manually.")


def validate(valX, valY, model):
    conf = model.conf
    if model.training: model.eval()
    # initialize
    bsz = valY.shape[0]
    # forward pass
    hidden = model.init_hidden(bsz)
    output, attention = model.forward(valX, hidden)
    # compute loss
    preds_raw = output.view(bsz, -1)
    preds = torch.argmax(preds_raw, dim=1)
    pure_loss = F.cross_entropy(preds_raw, valY).item()
    acc_score = acc(preds, valY)
    return pure_loss, acc_score




def train_v2(model, train_data, optimizer, iters, val_data=None, gpu=True):
    # initialize
    conf = model.conf
    start_time = time.time()
    n = len(train_data)
    total_loss = 0; total_pure_loss = 0; total_acc = 0
    I = Variable(torch.zeros(conf["batch_size"], conf["attn_hops"], conf["attn_hops"]))    
    for i in range(conf["batch_size"]):
        for j in range(conf["attn_hops"]):
            I.data[i][j][j] = 1

    # prepare validation data
    if val_data:
        with torch.no_grad():
#             val_data = random.sample(val_data, 1000)
            valX, valY = prepare_pred_batch(val_data, conf["voc"], 0, len(val_data), max_len=conf["max_length"])
            if gpu:
                valX = valX.cuda()
                valY = valY.cuda()
    val_loss, val_score, val_roc = (0., 0., 0.5)
    X, Y = prepare_train_batch(train_data, conf["voc"], 0, len(train_data), max_len=conf["max_length"])
    if gpu:
        X = X.cuda()
        Y = Y.cuda()

    criterion = nn.NLLLoss()

    try:
        for iter in range(iters):

            if not model.training: model.train()
            # prepare data
            st_idx = int((iter%(n//conf["batch_size"]))*conf["batch_size"])
            end_idx = st_idx + conf["batch_size"]
            trainX = X[:, st_idx:end_idx]
            trainY = Y[st_idx:end_idx]
            
            if trainY.shape[0] < conf["batch_size"]:
                continue
            
            # forward pass
            hidden = model.init_hidden(trainX.size(1))
            output, attention = model.forward(trainX, hidden)
            # compute loss
            loss = criterion(output.view(trainX.size(1), -1), trainY)
            total_pure_loss += loss.data
            attentionT = torch.transpose(attention, 1, 2).contiguous()
            extra_loss = Frobenius(torch.bmm(attention, attentionT) - I[:attention.size(0)])
            loss += conf["penalization_coeff"] * extra_loss
            # backprop
            optimizer.zero_grad()
            loss.backward()
            # clip gradient
            torch.nn.utils.clip_grad_norm_(model.parameters(), conf["clip"])
            # update
            optimizer.step()

            # log
            total_loss += loss.data.item()
            total_acc += acc(trainY, torch.argmax(output.view(trainX.size(1), -1), dim=1))
            if (iter+1) % conf["log_interval"] == 0 and iter > 0:
                if val_data and (iter+1)%conf["val_interval"] == 0: 
                    with torch.no_grad(): val_loss, val_score, val_roc = validate_in_batch(valX, valY, model, val_bsz=model.conf["val_bsz"])
                elapsed = time.time() - start_time
                print("iter {:3d} - loss {:5.4f} | pure loss {:5.4f} | acc {:.2%} | val loss {:5.4f} | val acc {:.2%} | val roc {:5.4f}".format(
                    iter, total_loss / conf["log_interval"], 
                    total_pure_loss / conf["log_interval"], total_acc / conf["log_interval"], 
                    val_loss, val_score, val_roc))
                total_loss = 0; total_pure_loss = 0; total_acc = 0
            # free up cache
            torch.cuda.empty_cache()
    
    except KeyboardInterrupt: print("\nTraining terminated manually.")


def validate_v2(valX, valY, model):
    conf = model.conf
    if model.training: model.eval()
    # initialize
    bsz = valY.shape[0]
    preds_raw = torch.zeros((bsz, model.conf["n_classes"]), requires_grad=False)
    for idx in range(bsz):
    # forward pass
        hidden = model.init_hidden(1)
        output, attention = model.forward(valX[:,idx].unsqueeze(1), hidden)
        # compute loss
        preds_raw[idx,:] = output.view(1, -1).detach()
        
    preds = torch.argmax(preds_raw, dim=1)
    pure_loss = F.nll_loss(preds_raw, valY).item()
    acc_score = acc(preds, valY)
    return pure_loss, acc_score


def validate_in_batch(valX, valY, model, val_bsz=512):
    conf = model.conf
    if model.training: model.eval()
    # initialize
    n = valY.shape[0]
    preds_raw = torch.zeros((n, model.conf["n_classes"]), requires_grad=False)
    for idx in range(n//val_bsz):
        st_idx = idx*val_bsz
        end_idx = st_idx+val_bsz
        X = valX[:, st_idx:end_idx]
        bsz = X.shape[1]
    # forward pass
        hidden = model.init_hidden(bsz)
        output, attention = model.forward(X, hidden)
        # compute loss
        preds_raw[st_idx:end_idx,:] = output.view(bsz, -1).detach()
        
    preds = torch.argmax(preds_raw, dim=1)
    pure_loss = F.nll_loss(preds_raw, valY).item()
    acc_score = acc(preds, valY)
    roc_score = roc_auc_score(valY.cpu().numpy(), preds_raw[:,1].cpu().numpy())
    return pure_loss, acc_score, roc_score



