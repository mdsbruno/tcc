import requests
import json
from datetime import datetime

def auth(tenant_id, client_id, client_secret):
    print('auth')
    auth_headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    auth_body = {
        "client_id": client_id,
        "client_secret": client_secret,
        "scope" : "https://storage.azure.com/.default",
        "grant_type" : "client_credentials"
    }
    resp = requests.post(f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token", headers=auth_headers, data=auth_body)
    return (resp.status_code, json.loads(resp.text))

def mkfs(account_name, fs_name, access_token):
    print('mkfs')
    fs_headers = {
        "Authorization": f"Bearer {access_token}"
    }
    resp = requests.put(f"https://{account_name}.dfs.core.windows.net/{fs_name}?resource=filesystem", headers=fs_headers)
    return (resp.status_code, resp.text)

def mkdir(account_name, fs_name, dir_name, access_token):
    print('mkdir')
    dir_headers = {
        "Authorization": f"Bearer {access_token}"
    }
    resp = requests.put(f"https://{account_name}.dfs.core.windows.net/{fs_name}/{dir_name}?resource=directory", headers=dir_headers)
    return (resp.status_code, resp.text)

def touch_file(account_name, fs_name, dir_name, file_name, access_token):
    print('touch_file')
    touch_file_headers = {
        "Authorization": f"Bearer {access_token}"
    }
    resp = requests.put(f"https://{account_name}.dfs.core.windows.net/{fs_name}/{dir_name}/{file_name}?resource=file", headers=touch_file_headers)
    return (resp.status_code, resp.text)

def append_file(account_name, fs_name, path, content, position, access_token):
    print('append_file')
    append_file_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "text/plain",
        "Content-Length": f"{len(content)}"
    }
    resp = requests.patch(f"https://{account_name}.dfs.core.windows.net/{fs_name}/{path}?action=append&position={position}", headers=append_file_headers, data=content)
    return (resp.status_code, resp.text)

def flush_file(account_name, fs_name, path, position, access_token):
    print('flush_file')
    flush_file_headers = {
        "Authorization": f"Bearer {access_token}"
    }
    resp = requests.patch(f"https://{account_name}.dfs.core.windows.net/{fs_name}/{path}?action=flush&position={position}", headers=flush_file_headers)
    return (resp.status_code, resp.text)

def mkfile(account_name, fs_name, dir_name, file_name, local_file_name, access_token):
    print('mkfile')
    status_code, result = touch_file(account_name, fs_name, dir_name, file_name, access_token)
    if status_code == 201:
        with open(local_file_name, 'rb') as local_file:
            path = f"{dir_name}/{file_name}"
            content = local_file.read()
            position = 0
            append_file(account_name, fs_name, path, content, position, access_token)
            position = len(content)
            flush_file(account_name, fs_name, path, position, access_token)
    else:
        print(result)

def mkfile_by_text(account_name, fs_name, dir_name, file_name, file_content, access_token):
    print('mkfile')
    status_code, result = touch_file(account_name, fs_name, dir_name, file_name, access_token)
    if status_code == 201:
        path = f"{dir_name}/{file_name}"
        content = file_content
        position = 0
        append_file(account_name, fs_name, path, content, position, access_token)
        position = len(content)
        flush_file(account_name, fs_name, path, position, access_token)
    else:
        print(result)

def uploadFile(filename, filecontent, filePath):
    tenant_id = 'codigo do tenant id'
    client_id = 'codigo do client id'
    client_secret = 'codigo de client secret'
    account_name = 'nome da account name'
    fs_name = 'data'
    now = datetime.now()
    dir_name = filePath+'/'+now.strftime("%Y")+'/'+now.strftime("%m")+'/'+now.strftime("%d")
    file_name = filename
    file_content = filecontent

    #Gera o token para criar link de acesso.
    auth_status_code, auth_result = auth(tenant_id, client_id, client_secret)
    access_token = auth_status_code == 200 and auth_result['access_token'] or ''
    print(access_token)
    
    #Cria um container caso não exista.
    mkfs_status_code, mkfs_result = mkfs(account_name, fs_name, access_token)
    print(mkfs_status_code, mkfs_result)

    #Cria o diretório com as datas atuais.
    mkdir_status_code, mkdir_result = mkdir(account_name, fs_name, dir_name, access_token)
    print(mkdir_status_code, mkdir_result)

    #Cria o arquivo no data lake.
    mkfile_by_text(account_name, fs_name, dir_name, file_name, file_content, access_token)