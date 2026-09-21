import asyncio
import json
import websockets
import os
from websockets.exceptions import ConnectionClosed

clients = {}  # ws -> dict
squads = {}   # squad_id -> dict
leaderboard = []

next_client_id = 1
next_squad_id = 100

async def broadcast_lobby():
    squad_list = [
        {"id": s_id, "name": s["name"], "status": s["status"]} 
        for s_id, s in squads.items()
    ]
    msg = json.dumps({
        "type": "LOBBY_STATE", 
        "squads": squad_list, 
        "leaderboard": leaderboard[:10]
    })
    
    # Envia para todos os clientes ativos de forma segura
    for ws in list(clients.keys()):
        try:
            await ws.send(msg)
        except ConnectionClosed:
            pass
        except Exception as e:
            print(f"[AVISO] Erro ao enviar broadcast: {e}")

async def handler(websocket):
    global next_client_id, next_squad_id
    c_id = next_client_id
    next_client_id += 1
    clients[websocket] = {"id": c_id, "name": f"Cadete_{c_id}", "squadId": None}
    
    print(f"[CONECTADO] Cadete_{c_id} entrou no Salão.")
    
    try:
        await websocket.send(json.dumps({"type": "WELCOME", "myId": c_id}))
        await broadcast_lobby()

        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                continue

            action = data.get("action")
            user = clients.get(websocket)
            if not user:
                continue

            if action == "CREATE_SQUAD":
                s_id = f"CASP-{next_squad_id}"
                next_squad_id += 1
                user["squadId"] = s_id
                squads[s_id] = {
                    "id": s_id,
                    "name": data.get("squadName", "Esquadrão"),
                    "host_ws": websocket,
                    "client_ws": None,
                    "status": "WAITING"
                }
                print(f"[GRUPO CRIADO] {squads[s_id]['name']} ({s_id})")
                await broadcast_lobby()

            elif action == "JOIN_SQUAD":
                s_id = data.get("squadId")
                if s_id in squads and squads[s_id]["status"] == "WAITING":
                    sq = squads[s_id]
                    sq["client_ws"] = websocket
                    sq["status"] = "PLAYING"
                    user["squadId"] = s_id
                    
                    msg_h = json.dumps({"type": "GAME_START", "role": "HOST", "squadName": sq["name"]})
                    msg_c = json.dumps({"type": "GAME_START", "role": "CLIENT", "squadName": sq["name"]})
                    try:
                        await sq["host_ws"].send(msg_h)
                        await sq["client_ws"].send(msg_c)
                    except ConnectionClosed:
                        pass
                    print(f"[PARTIDA INICIADA] {sq['name']}")
                    await broadcast_lobby()

            elif action == "GAME_EVENT":
                s_id = user.get("squadId")
                if s_id in squads:
                    sq = squads[s_id]
                    other_ws = sq["client_ws"] if websocket == sq["host_ws"] else sq["host_ws"]
                    if other_ws:
                        try:
                            await other_ws.send(json.dumps({
                                "type": "PEER_EVENT",
                                "event": data.get("event"),
                                "payload": data.get("payload")
                            }))
                        except ConnectionClosed:
                            pass

            elif action == "FINISH_GAME":
                s_id = user.get("squadId")
                if s_id in squads:
                    sq = squads[s_id]
                    leaderboard.append({
                        "squad": sq["name"], 
                        "score": data.get("score", 0), 
                        "date": "Concluído"
                    })
                    leaderboard.sort(key=lambda x: x["score"], reverse=True)
                    print(f"[MISSÃO CUMPRIDA] {sq['name']} - {data.get('score')} pts")
                    del squads[s_id]
                    await broadcast_lobby()

    except ConnectionClosed:
        # Captura desconexão normal ou fechamento abrupto da janela
        pass
    except Exception as e:
        print(f"[ERRO] Exceção no handler: {e}")
    finally:
        user = clients.get(websocket, {})
        s_id = user.get("squadId")
        if s_id in squads:
            sq = squads[s_id]
            other_ws = sq["client_ws"] if websocket == sq["host_ws"] else sq["host_ws"]
            if other_ws:
                try:
                    await other_ws.send(json.dumps({"type": "PARTNER_DISCONNECTED"}))
                except ConnectionClosed:
                    pass
            del squads[s_id]
            
        cadete_id = user.get('id', '?')
        clients.pop(websocket, None)
        print(f"[DESCONECTADO] Cadete_{cadete_id} saiu.")
        await broadcast_lobby()

async def main():
    # O Railway injeta a porta na variável de ambiente PORT
    port = int(os.environ.get("PORT", 8080))
    print(f"[SALÃO CASPIAN] Servidor Python ativo na porta {port}...")
    async with websockets.serve(handler, "0.0.0.0", port):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())