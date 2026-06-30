# Pineapple Algorithmic Trading Platform

Clean setup and migration of the Trading Infrastructure Platform.

## Network Architecture
- **Subnet:** `10.100.100.0/24`
- **Controller/Brain:** `10.100.100.200`
- **TimescaleDB Data:** `10.100.100.201`
- **Compute (Live):** `10.100.100.202`
- **Lab (JupyterHub/Research):** `10.100.100.203`
- **Perimeter Router:** `10.100.100.254`

## Sync Workflow
We use a seamless double-hop sync workflow:
1. Work locally on the Windows host (`c:\Users\Trader01\proyectos\pineapple`).
2. Push changes to the controller VM:
   ```bash
   git push vm main
   ```
3. A `post-receive` Git hook on the VM automatically pushes the updates to the GitHub repository:
   ```bash
   git push origin main
   ```
