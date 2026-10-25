// Silah Çılgınlığı - online sunucu (ek kütüphane gerektirmez, Node 18+)
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const PORT=process.env.PORT||3000,HTML=path.join(__dirname,'public','index.html'),MAX=12,GUID='258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const rooms=new Map();
const srv=http.createServer((q,r)=>{
  if(q.url==='/health'){r.end('ok');return}
  fs.readFile(HTML,(e,d)=>{
    if(e){r.writeHead(500);r.end('public/index.html bulunamadi');return}
    r.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-cache'});r.end(d)})});
function frame(op,buf){const n=buf.length;let h;
  if(n<126)h=Buffer.from([0x80|op,n]);
  else if(n<65536){h=Buffer.alloc(4);h[0]=0x80|op;h[1]=126;h.writeUInt16BE(n,2)}
  else{h=Buffer.alloc(10);h[0]=0x80|op;h[1]=127;h.writeBigUInt64BE(BigInt(n),2)}
  return Buffer.concat([h,buf])}
const send=(c,s)=>{if(!c.dead)try{c.sock.write(frame(1,Buffer.from(s)))}catch(e){}};
srv.on('upgrade',(req,sock)=>{
  const u=new URL(req.url,'http://x'),key=req.headers['sec-websocket-key'];
  if(u.pathname!=='/ws'||!key){sock.destroy();return}
  const rn=(u.searchParams.get('room')||'x').toLowerCase().slice(0,40);
  let room=rooms.get(rn);if(!room)rooms.set(rn,room=new Map());
  if(room.size>=MAX){sock.end('HTTP/1.1 503 Service Unavailable\r\n\r\n');return}
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+crypto.createHash('sha1').update(key+GUID).digest('base64')+'\r\n\r\n');
  sock.setNoDelay(true);
  const id=crypto.randomBytes(4).toString('hex'),c={sock,st:{},dead:false,buf:Buffer.alloc(0)};
  const peers={};for(const[k,o]of room)peers[k]=o.st;
  room.set(id,c);send(c,JSON.stringify({t:'hello',id,peers}));
  const close=()=>{if(c.dead)return;c.dead=true;room.delete(id);const m=JSON.stringify({t:'l',id});for(const o of room.values())send(o,m);if(!room.size)rooms.delete(rn);try{sock.destroy()}catch(e){}};
  const onMsg=s=>{let m;try{m=JSON.parse(s)}catch(e){return}
    if(!m||m.t!=='p'||!m.d||typeof m.d!=='object')return;
    Object.assign(c.st,m.d);const out=JSON.stringify({t:'p',id,d:m.d});
    for(const[k,o]of room)if(k!==id)send(o,out)};
  sock.on('data',d=>{c.buf=Buffer.concat([c.buf,d]);if(c.buf.length>30000){close();return}
    for(;;){const b=c.buf;if(b.length<2)break;
      const op=b[0]&15,masked=b[1]&128;let len=b[1]&127,off=2;
      if(len===126){if(b.length<4)break;len=b.readUInt16BE(2);off=4}
      else if(len===127){if(b.length<10)break;len=Number(b.readBigUInt64BE(2));off=10}
      if(!masked||len>8000){close();return}
      if(b.length<off+4+len)break;
      const mk=b.subarray(off,off+4),p=Buffer.from(b.subarray(off+4,off+4+len));
      for(let i=0;i<len;i++)p[i]^=mk[i&3];
      c.buf=b.subarray(off+4+len);
      if(op===8){close();return}
      else if(op===9)sock.write(frame(10,p));
      else if(op===1)onMsg(p.toString())}});
  sock.on('close',close);sock.on('error',close)});
setInterval(()=>{for(const room of rooms.values())for(const c of room.values())if(!c.dead)try{c.sock.write(frame(9,Buffer.alloc(0)))}catch(e){}},25000);
srv.listen(PORT,()=>console.log('Oyun hazir: http://localhost:'+PORT));
