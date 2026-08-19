"""Small benchmark client for the Shardkeep API.
Usage: python benchmark.py http://127.0.0.1:5000 username password
"""
import sys, time, requests

if len(sys.argv) != 4:
    print("Usage: python benchmark.py BASE_URL USERNAME PASSWORD"); raise SystemExit(2)
base,user,pw=sys.argv[1:]
s=requests.Session()
t=time.perf_counter(); r=s.post(base.rstrip('/')+'/api/login',json={'login_id':user,'password':pw}); login_ms=(time.perf_counter()-t)*1000
if not r.ok: print('Login failed:',r.text); raise SystemExit(1)
t=time.perf_counter(); d=s.get(base.rstrip('/')+'/api/benchmark').json(); api_ms=(time.perf_counter()-t)*1000
print(f'Login: {login_ms:.1f} ms')
print(f'Benchmark API: {api_ms:.1f} ms')
print(f'Cluster capacity: {d["total_capacity_bytes"]/(1024**3):.1f} GB')
print(f'Used: {d["used_bytes"]/(1024**3):.3f} GB ({d["raw_utilization_pct"]:.2f}%)')
print(f'Cloud mode: {d["cloud_mode"]}')
for x in d['placement_samples']: print(f'Placement x10 for {x["sample_bytes"]//1024} KB: {x["placement_ms_10x"]:.2f} ms')
for n in d['nodes']: print(f'{n["name"]} {n["rack"]}: {n["utilization_pct"]:.2f}% used')
