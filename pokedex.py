# %%
import requests
import json
# %%
pokemon_call= requests.get('https://pokeapi.co/api/v2/pokemon/1/')
# %%
print(pokemon_call)
# %%
print(type(pokemon_call))
# %%
print(pokemon_call.content)
data = pokemon_call.json()
# %%
print(type(data))
# %%
processed_ids = set()
mons = []
if pokemon_call.status_code != 200:
    print(pokemon_call.text)
else:
    for item in data:
        record_id = item[int('id')]
        if record_id not in processed_ids:
            print(data['id'],":",data['name'],":",data['types'][0]['type']['name'],":",data['types'][1]['type']['name'],":",data['height'],":", data['weight'])
            mons.append(item)
        processed_ids.add(record_id)
        
# %%
for record in mons:
    print(record)
# %%
print(mons)


# %%
height_value = data['height']
weight_value = data['weight']
id_value = data['id']
name_value = data['name']
types_value1 = data['types'][0]['type']['name']
types_value2 = data['types'][1]['type']['name']

print(types_value2)
# %%

# %%
