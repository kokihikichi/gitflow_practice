from ..imports import *
from .utils import *

def predict(data, model, gpu=True, val_bsz=512):
    conf = model.conf
    # convert data to tensors
    with torch.no_grad():
        valX, valY = prepare_pred_batch(data, conf["voc"], 0, len(data), max_len=conf["max_length"])
        if gpu:
            valX = valX.cuda()
            valY = valY.cuda()
    
    if model.training: model.eval()
    # initialize
#     for 
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
    print('Test evaluation | pure loss {:5.4f} | acc {:.2%}'.format(pure_loss, acc_score))
    # free up cache
    torch.cuda.empty_cache()
    return preds_raw, preds, attention



