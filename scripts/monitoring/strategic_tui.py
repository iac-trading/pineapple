import os
import json
import asyncio
import logging
from datetime import datetime
from nats.aio.client import Client as NATS
from rich.console import Console, Group
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text

console = Console()

class StrategicTUI:
    def __init__(self):
        self.nc = NATS()
        self.strategies = {} # instance_id -> status_dict
        self.insider_signals = []
        self.mm_quotes = {} # symbol -> quote_dict
        self.macro_regime = "SEARCHING..."
        self.nats_url = os.environ["NATS_URL"]

    async def start(self):
        await self.nc.connect(servers=[self.nats_url])
        
        # Discover strategies
        asyncio.create_task(self.discovery_loop())
        
        # Listen to signals
        await self.nc.subscribe("intelligence.insider", cb=self.on_insider_signal)
        await self.nc.subscribe("factory.mm.quotes.*", cb=self.on_mm_quote)
        await self.nc.subscribe("intelligence.regime.master", cb=self.on_regime)
        
        with Live(self.generate_layout(), refresh_per_second=2, screen=True) as live:
            while True:
                await asyncio.sleep(0.5)
                live.update(self.generate_layout())

    async def discovery_loop(self):
        """Periodically ping strategies for health/status."""
        def on_ping_response(msg):
            try:
                data = json.loads(msg.data.decode())
                self.strategies[data['instance_id']] = data
            except Exception:
                pass

        while True:
            try:
                reply_inbox = self.nc.new_inbox()
                sub = await self.nc.subscribe(reply_inbox, cb=on_ping_response)
                await self.nc.publish("factory.control.ping", b"", reply=reply_inbox)
                
                await asyncio.sleep(1.5)
                await sub.unsubscribe()
            except Exception:
                pass
            await asyncio.sleep(10)

    async def on_insider_signal(self, msg):
        data = json.loads(msg.data.decode())
        self.insider_signals.append(data)
        if len(self.insider_signals) > 10:
            self.insider_signals.pop(0)

    async def on_mm_quote(self, msg):
        data = json.loads(msg.data.decode())
        symbol = data.get("symbol")
        self.mm_quotes[symbol] = data

    async def on_regime(self, msg):
        data = json.loads(msg.data.decode())
        self.macro_regime = data.get("regime", "UNKNOWN")

    def generate_layout(self):
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=3)
        )
        layout["main"].split_row(
            Layout(name="left", ratio=1),
            Layout(name="right", ratio=1)
        )
        
        # Header
        header_text = Text(f"🚀 AXIO-QUANT STRATEGIC CONTROL CENTER | REGIME: {self.macro_regime}", style="bold white on blue", justify="center")
        layout["header"].update(Panel(header_text))
        
        # Left: Strategy Table
        strat_table = Table(title="🛡️ ACTIVE ALPHA ENGINES")
        strat_table.add_column("Strategy")
        strat_table.add_column("Symbol")
        strat_table.add_column("Instance ID")
        strat_table.add_column("Status")
        
        # For now, we manually mock or add if we received any response (logic to be improved)
        # In a real TUI we'd have a persistent set of instances
        for instance_id, info in self.strategies.items():
            s_name = str(info.get('strategy', 'Unknown'))
            s_sym = str(info.get('symbol', 'N/A'))
            strat_table.add_row(s_name, s_sym, str(instance_id)[:8], "[green]RUNNING[/green]")
            
        layout["left"].update(Panel(strat_table))
        
        # Right TOP: MM Quotes
        mm_table = Table(title="⚡ MARKET MAKER LIVE FEED")
        mm_table.add_column("Symbol")
        mm_table.add_column("Bid")
        mm_table.add_column("Ask")
        mm_table.add_column("Inv (Q)")
        
        for sym, q in self.mm_quotes.items():
            mm_table.add_row(sym, f"{q['bid']:.2f}", f"{q['ask']:.2f}", f"{q['q']:.4f}")
            
        # Right BOTTOM (in a sub-layout if we wanted, but let's just stack)
        insider_table = Table(title="🔍 SEC INSIDER SIGNALS")
        insider_table.add_column("Symbol")
        insider_table.add_column("Owner")
        insider_table.add_column("Value (USD)")
        
        for sig in reversed(self.insider_signals):
            insider_table.add_row(sig['symbol'], sig['insider'][:15], f"${sig['value_usd']:,.0f}")
            
        layout["right"].update(Panel(Group(mm_table, insider_table)))
        
        # Footer
        footer_text = f"NATS: {self.nats_url} | Time: {datetime.now().strftime('%H:%M:%S')} | Press Ctrl+C to exit"
        layout["footer"].update(Panel(footer_text, style="dim"))
        
        return layout

if __name__ == "__main__":
    tui = StrategicTUI()
    try:
        asyncio.run(tui.start())
    except KeyboardInterrupt:
        pass
