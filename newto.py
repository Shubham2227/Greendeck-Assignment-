import pandas as pd
from functools import partial
from flask_cors import CORS
from flask import request,render_template
import gdown
import flask
import io
import os

url = 'https://drive.google.com/a/greendeck.co/uc?id=19r_vn0vuvHpE-rJpFHvXHlMvxa8UOeom&export=download'

app = flask.Flask(__name__)
CORS(app)

class prepare_dataset:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = None
        self.simi_list = []
        
    def load_file(self):
        self.df = pd.read_json(self.file_path, lines=True, orient='columns')
    
    def make_attr(self):
        self.df['_id'] = self.df['_id'].apply(lambda x: x['$oid'])
        self.df['brand.name'] = self.df['brand'].apply(lambda x: x['name'])
        self.df['rank'] = self.df['positioning'].apply(lambda x: None if isinstance(x, float) else x['rank'])

        self.df['stock_availability'] = self.df['stock'].apply(lambda x: x['available']).astype(int)

        self.df['offer_price'] = self.df['price'].apply(lambda x: x['offer_price']['value'])
        self.df['regular_price'] = self.df['price'].apply(lambda x:  x['regular_price']['value'])
        self.df['basket_price'] = self.df['price'].apply(lambda x:  x['basket_price']['value'])

        self.df['discount'] = (abs(self.df['regular_price'] - self.df['offer_price'])/self.df['regular_price'])*100
        
        self.df.drop(['created_at', 'description_text', 'lv_url', 'media', 'meta', 'positioning',
                     'price', 'price_changes', 'price_positioning', 'sizes', 'sku', 'spider', 'stock', 
                    "url", "website_id", 'updated_at', 'classification', 'name', 'brand', 'price_positioning_text'], axis=1, inplace=True)
        
    def extract_similar_products(self, data):
        if isinstance(data, float):
            return None
        data = data['website_results']
        res = {}
        for key in data.keys():
            try:
                if "_source" in data[key].keys():
                    res[key] = data[key]['_source']

                elif "knn_items" in data[key].keys() and "_source" in data[key]['knn_items'][0].keys():
                    res[key] = data[key]['knn_items'][0]['_source']
            except:
                continue
        return res

    def extract_detail(self, data):
        if data[1] is None or isinstance(data[1], float):
            return
        ids, data = data[0], data[1]
        for key in data.keys():
            self.simi_list.append({'_id': ids, "competitor": key, "cp_brand": data[key]['brand']['name'],
                                'cp_offer_price': data[key]['price']['offer_price']['value'],
                                'cp_regular_price': data[key]['price']['regular_price']['value'],
                                'cp_basket_price': data[key]['price']['basket_price']['value']})

    def preprocess(self):
        self.load_file()
        self.make_attr()
        
        temp = self.df[['_id', 'similar_products']].copy()
        temp['similar_prod_dict'] = temp['similar_products'].apply(self.extract_similar_products)
        temp.loc[temp['similar_prod_dict']=={}] = None
        temp[["_id", "similar_prod_dict"]].apply(self.extract_detail, axis=1)

        simi_df = pd.DataFrame(self.simi_list)
        simi_df['cp_discount'] = (abs(simi_df['cp_offer_price'] - simi_df['cp_regular_price']) \
                                       / simi_df['cp_regular_price'])*100 
        
        self.df = self.df.merge(simi_df, how='left', on='_id')
        self.df.drop("similar_products", axis=1, inplace=True)
        self.df['discount_diff'] = (abs(self.df['basket_price'] - self.df['cp_basket_price'])/self.df['basket_price'])*100
        
        return self.df.copy()
    

def inp_operation(inp):
    if inp['operator']==">":
        res = df.loc[df[inp['operand1']] > inp['operand2']].copy()
    elif inp['operator']=="<":
        res = df.loc[df[inp['operand1']] < inp['operand2']].copy()
    else:
        res = df.loc[df[inp['operand1']] == inp['operand2']].copy()
    res = res.drop_duplicates(subset=['_id'])
    return res

def validate(res):
    if res.shape[0]==0:
        return True
    return False

def discounted_products_list(inp):
    inp = inp[0]
    res = inp_operation(inp)
    if validate(res):
        return {"Detail": "No Matching products found with this operation!"}
    return {"discounted_products_list": res['_id'].to_list()}

def discounted_products_count(inp):
    inp = inp[0]
    res = inp_operation(inp)
    if validate(res):
        return {"Detail": "No Matching products found with this operation!"}
    return {"products_count": res['_id'].count(), "discount_avg": res['discount'].mean()}

def expensive_list(inp, df):
    if len(inp)==0:
        res = list(df.loc[df['basket_price']>df['cp_basket_price']]['_id'].unique())
    else:
        inp = inp[0]
        res = inp_operation(inp)
        if validate(res):
            return {"Detail": "No Matching products found with this operation!"}
        res = res.loc[res['basket_price']>res['cp_basket_price']]['_id'].to_list()
    return {"expensive_list": res}

def competition_discount_diff_list(inp):
    f1, f2 = inp[0], inp[1]
    if f1["operand1"]!='discount_diff':
        f1, f2 = f2, f1
    res = inp_operation(f1)
    res = res.loc[res['cp_id']==f2['operand2']]["_id"].to_list()
    if len(res)==0:
            return {"Detail": "No Matching products found with this operation!"}
    return {"competition_discount_diff_list": res}

def process_request_1(query, df):
    if query['query_type'].find("|")<0:
        query['query_type'] = query['query_type'].split("|")[0]
    else:
        pass
    
    if query['query_type']=='expensive_list':
        res = globals()[query['query_type']](query.get('filters', []), df) 
    else:
        res = globals()[query['query_type']](query.get('filters', [])) 
    return res


def init_files(dump_path = 'dumps/netaporter_gb.json'):
    if dump_path.split('/')[0] not in os.listdir():
        os.mkdir(dump_path.split('/')[0])
    if os.path.exists(dump_path):
        pass
    else:
        gdown.download(url = url, output = dump_path, quiet=False)
@app.route('/')
def index():
    return render_template('query.html')     
@app.route('/details',methods = ['POST', 'GET'])
def details():
    # init_files('dumps/netaporter_gb.json')
    # obj = prepare_dataset('dumps/netaporter_gb.json')
    # df = obj.preprocess()
    # query = { "query_type": "discounted_products_list", "filters": [{ "operand1": "discount", "operator": ">", "operand2": 5 }] }
    query = request.form['query']
    print(query)
    queryS = str(query)
    res = process_request_1(eval(queryS), df)
    print(res)
    return render_template('products.html', res=res)

if __name__ == '__main__':
    init_files('dumps/netaporter_gb.json')
    obj = prepare_dataset('dumps/netaporter_gb.json')
    df = obj.preprocess()   
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True,host='0.0.0.0',port=port)
