#!/usr/bin/env python3
from flask import Flask, request, jsonify
import requests
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import threading
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

AURORA_CONFIG = {
    'tenant_id': '034b1c47-310a-460f-9d5d-b625dd354f12',
    'api_key': os.environ.get('AURORA_API_KEY', 'rk_prod_eff6929551ffeb939d848ef0'),
    'base_url': 'https://api.aurorasolar.com'
}

QUICKBASE_CONFIG = {
    'realm': 'kin.quickbase.com',
    'user_token': os.environ.get('QUICKBASE_TOKEN', 'b6um6p_p3bs_0_bmrupwzbc82cdnb44a7pirtbxif'),
    'app_id': 'bvdg9ck3u',
    'table_id': os.environ.get('QUICKBASE_TABLE_ID', '')
}

class AuroraSolarClient:
    def __init__(self):
        self.tenant_id = AURORA_CONFIG['tenant_id']
        self.api_key = AURORA_CONFIG['api_key']
        self.base_url = AURORA_CONFIG['base_url']
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
    
    def get_design_summary(self, design_id: str) -> Optional[Dict]:
        url = f"{self.base_url}/tenants/{self.tenant_id}/designs/{design_id}/summary"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching design {design_id}: {str(e)}")
            return None
    
    def get_project_designs(self, project_id: str) -> Optional[List[str]]:
        url = f"{self.base_url}/tenants/{self.tenant_id}/projects/{project_id}/designs"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            designs = response.json().get('designs', [])
            return [d.get('design_id') for d in designs if d.get('design_id')]
        except Exception as e:
            logger.error(f"Error fetching designs for project {project_id}: {str(e)}")
            return None

class QuickbaseClient:
    def __init__(self):
        self.realm = QUICKBASE_CONFIG['realm']
        self.user_token = QUICKBASE_CONFIG['user_token']
        self.app_id = QUICKBASE_CONFIG['app_id']
        self.table_id = QUICKBASE_CONFIG['table_id']
        self.headers = {
            'QB-Realm-Hostname': self.realm,
            'Authorization': f'QB-USER-TOKEN {self.user_token}',
            'Content-Type': 'application/json'
        }
    
    def upsert_record(self, data: Dict[int, Any]) -> bool:
        if not self.table_id:
            logger.error("Table ID not set")
            return False
        
        url = f"https://api.quickbase.com/v1/records"
        body = {
            'to': self.table_id,
            'data': [data],
            'mergeFieldId': 6,
            'fieldsToReturn': [6]
        }
        
        try:
            response = requests.post(url, json=body, headers=self.headers)
            response.raise_for_status()
            logger.info(f"Successfully upserted record")
            return True
        except Exception as e:
            logger.error(f"Error upserting record: {str(e)}")
            return False

def transform_data(design_data: Dict) -> Dict[int, Any]:
    qb_record = {}
    design = design_data.get('design', {})
    
    qb_record[6] = {'value': design.get('design_id', '')}
    qb_record[7] = {'value': design.get('project_id', '')}
    qb_record[21] = {'value': design.get('system_size_stc', 0) / 1000}
    qb_record[22] = {'value': design.get('system_size_ac', 0) / 1000}
    qb_record[16] = {'value': 'Installed'}
    qb_record[18] = {'value': 'Installed'}
    qb_record[9] = {'value': datetime.now(timezone.utc).isoformat()}
    
    energy = design.get('energy_production', {})
    if energy:
        qb_record[51] = {'value': energy.get('annual', 0)}
        qb_record[52] = {'value': str(energy.get('annual_offset', '0').replace('%', ''))}
        qb_record[45] = {'value': energy.get('up_to_date', False)}
        if energy.get('monthly'):
            qb_record[53] = {'value': json.dumps(energy['monthly'])[:5000]}
    
    arrays = design.get('arrays', [])
    if arrays:
        qb_record[19] = {'value': len(arrays)}
        total_modules = 0
        landscape = 0
        portrait = 0
        
        for array in arrays:
            module = array.get('module', {})
            if module:
                count = module.get('count', 0)
                total_modules += count
                if array.get('orientation') == 'landscape':
                    landscape += count
                elif array.get('orientation') == 'portrait':
                    portrait += count
                    
                if total_modules == count:
                    qb_record[46] = {'value': module.get('name', '')}
                    qb_record[61] = {'value': module.get('id', '')}
                    qb_record[62] = {'value': module.get('rating_stc', 0)}
                    qb_record[47] = {'value': module.get('rating_stc', 0)}
            
            microinverter = array.get('microinverter', {})
            if microinverter:
                qb_record[57] = {'value': microinverter.get('id', '')}
                qb_record[58] = {'value': microinverter.get('name', '')}
                qb_record[59] = {'value': microinverter.get('count', 0)}
                qb_record[60] = {'value': microinverter.get('rated_power', 0)}
            
            dc_optimizer = array.get('dc_optimizer', {})
            if dc_optimizer:
                qb_record[63] = {'value': dc_optimizer.get('id', '')}
                qb_record[64] = {'value': dc_optimizer.get('name', '')}
                qb_record[71] = {'value': dc_optimizer.get('count', 0)}
        
        qb_record[20] = {'value': total_modules}
        qb_record[30] = {'value': total_modules}
        qb_record[24] = {'value': landscape}
        qb_record[23] = {'value': portrait}
        
        solar_access = sum(a.get('shading', {}).get('solar_access', {}).get('annual', 0) for a in arrays)
        tsrf = sum(a.get('shading', {}).get('total_solar_resource_fraction', {}).get('annual', 0) for a in arrays)
        if len(arrays) > 0:
            qb_record[65] = {'value': solar_access / len(arrays)}
            qb_record[67] = {'value': tsrf / len(arrays)}
        
        qb_record[29] = {'value': json.dumps(arrays)[:1000]}
    
    string_inverters = design.get('string_inverters', [])
    if string_inverters:
        qb_record[25] = {'value': len(string_inverters)}
        if string_inverters:
            inv = string_inverters[0]
            qb_record[48] = {'value': inv.get('name', '')}
            qb_record[54] = {'value': inv.get('id', '')}
            qb_record[56] = {'value': inv.get('rated_power', 0)}
    
    bom = design.get('bill_of_materials', [])
    if bom:
        qb_record[44] = {'value': len(bom)}
        
        for item in bom:
            ct = item.get('component_type')
            sku = item.get('sku', '')
            mfg = item.get('manufacturer_name', '')
            qty = item.get('quantity', 0)
            
            if ct == 'modules':
                qb_record[80] = {'value': sku}
                qb_record[68] = {'value': mfg}
            elif ct == 'inverters':
                qb_record[81] = {'value': sku}
                qb_record[69] = {'value': mfg}
            elif ct == 'microinverters':
                qb_record[83] = {'value': sku}
                qb_record[72] = {'value': mfg}
                qb_record[27] = {'value': qty}
            elif ct == 'dc_optimizers':
                qb_record[82] = {'value': sku}
                qb_record[70] = {'value': mfg}
                qb_record[26] = {'value': qty}
            elif ct == 'batteries':
                qb_record[85] = {'value': sku}
            elif ct == 'combiner_boxes':
                qb_record[86] = {'value': sku}
                qb_record[36] = {'value': qty}
            elif ct == 'disconnects':
                qb_record[87] = {'value': sku}
                qb_record[37] = {'value': qty}
            elif ct == 'racking_components':
                qb_record[84] = {'value': sku}
                qb_record[32] = {'value': qty}
        
        qb_record[73] = {'value': json.dumps(bom)[:10000]}
    
    qb_record[10] = {'value': json.dumps(design_data)[:10000]}
    
    return qb_record

def process_webhook(data: Dict):
    try:
        project_id = data.get('project_id')
        logger.info(f"Processing webhook for project {project_id}")
        
        aurora_client = AuroraSolarClient()
        qb_client = QuickbaseClient()
        
        design_ids = aurora_client.get_project_designs(project_id)
        if not design_ids:
            logger.warning(f"No designs found for project {project_id}")
            return
        
        logger.info(f"Found {len(design_ids)} designs to sync")
        
        for design_id in design_ids:
            try:
                design_data = aurora_client.get_design_summary(design_id)
                if design_data:
                    qb_record = transform_data(design_data)
                    qb_client.upsert_record(qb_record)
                    logger.info(f"Synced design {design_id}")
                time.sleep(0.7)
            except Exception as e:
                logger.error(f"Error processing design {design_id}: {e}")
                
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'service': 'Aurora Solar Webhook Server',
        'status': 'running',
        'endpoints': {
            'webhook': '/webhook',
            'health': '/health'
        }
    })

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy'})

@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET':
        data = request.args.to_dict()
    else:
        data = request.get_json() or {}
    
    logger.info(f"Received webhook: {json.dumps(data)}")
    
    thread = threading.Thread(target=process_webhook, args=(data,))
    thread.daemon = True
    thread.start()
    
    return jsonify({'status': 'accepted'}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
