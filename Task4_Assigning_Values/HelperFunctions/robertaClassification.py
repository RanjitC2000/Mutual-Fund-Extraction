from transformers import RobertaForSequenceClassification, RobertaTokenizer
import torch

model = RobertaForSequenceClassification.from_pretrained('roberta-base', num_labels=2)
model.load_state_dict(torch.load("Task4/Roberta/roberta-model.pth", map_location=torch.device('cpu')))

device = torch.device("cpu")
model.to(device)

def roberta_filter(paragraph, field):
  tokenizer = RobertaTokenizer.from_pretrained('roberta-base', do_lower_case=True)
  result={}

  paragraph_encoding = tokenizer([paragraph], truncation=True, padding=True)

  input_ids = torch.tensor(paragraph_encoding['input_ids'])
  attention_mask = torch.tensor(paragraph_encoding['attention_mask'])

  input_ids.to(device)
  attention_mask.to(device)

  outputs = model(input_ids, attention_mask=attention_mask)
  x = outputs['logits'][0][0].item()
  y = outputs['logits'][0][1].item()

  result = {'status': False, 'Paragraph': paragraph}
  if(y >= x):
    result['status'] = True
  else:
    result['status'] = False

  return result