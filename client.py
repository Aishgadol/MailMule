import random
import tkinter as tk
from tkinter import messagebox
from server import handle_request  # your server script

class App:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Local RAG Email Search")
        self.root.geometry("1200x800")
        self.root.resizable(False, False)

        self.signup_mode_enabled = False
        self.credentials = {}

        self._build_login_screen()
        self._build_search_screen()
        self.show_login_screen()

    def _build_login_screen(self):
        self.login_frame = tk.Frame(self.root)

        # title
        self.login_label = tk.Label(self.login_frame, text="Login", font=("Arial", 24))
        self.login_label.pack(pady=20)

        # username & password
        self.username_var = tk.StringVar()
        self.password_var = tk.StringVar()
        tk.Label(self.login_frame, text="Username:").pack(anchor="w", padx=20)
        tk.Entry(self.login_frame, textvariable=self.username_var).pack(fill="x", padx=20)
        tk.Label(self.login_frame, text="Password:").pack(anchor="w", padx=20, pady=(10,0))
        pwd_entry = tk.Entry(self.login_frame, textvariable=self.password_var, show="*")
        pwd_entry.pack(fill="x", padx=20)
        pwd_entry.bind("<Return>", lambda e: self.login_button.invoke())

        # buttons row
        btn_frame = tk.Frame(self.login_frame)
        btn_frame.pack(pady=20)
        tk.Button(
            btn_frame,
            text="Forgot password?",
            command=self._on_forgot
        ).pack(side="left", padx=10)
        self.signup_button = tk.Button(
            btn_frame,
            text="Sign up!",
            command=self._toggle_signup
        )
        self.signup_button.pack(side="left", padx=10)

        # LOGIN / SIGN UP button at bottom center
        self.login_button = tk.Button(
            self.login_frame,
            text="LOGIN",
            font=("Arial", 14),
            command=self._on_login
        )
        self.login_button.pack(pady=30)

    def _build_search_screen(self):
        self.search_frame = tk.Frame(self.root)

        # top bar: back, query, search
        top = tk.Frame(self.search_frame)
        tk.Button(top, text="< Back", command=self._on_back).pack(side="left", padx=5)
        self.query_entry = tk.Entry(top)
        self.query_entry.pack(side="left", fill="x", expand=True, padx=5)
        search_btn = tk.Button(top, text="Search", command=self._on_search)
        search_btn.pack(side="left", padx=5)
        top.pack(fill="x", pady=10, padx=10)

        # content area
        content = tk.Frame(self.search_frame)
        content.pack(fill="both", expand=True, padx=10, pady=10)

        # attributes panel (left)
        attr_container = tk.Frame(content, bd=1, relief="groove")
        attr_container.pack(side="left", fill="y", padx=(0,10))
        tk.Label(attr_container, text="Attributes", font=("Arial", 12, "bold")).pack(anchor="nw", pady=5)
        self._attr_canvas = tk.Canvas(attr_container, width=300)
        self._attr_canvas.pack(side="left", fill="y", expand=True)
        attr_scroll = tk.Scrollbar(attr_container, orient="vertical", command=self._attr_canvas.yview)
        attr_scroll.pack(side="right", fill="y")
        self._attr_canvas.configure(yscrollcommand=attr_scroll.set)
        self.attr_frame = tk.Frame(self._attr_canvas)
        self._attr_canvas.create_window((0,0), window=self.attr_frame, anchor="nw")
        self.attr_frame.bind("<Configure>", lambda e: self._attr_canvas.configure(scrollregion=self._attr_canvas.bbox("all")))

        # results panel (right)
        res_container = tk.Frame(content, bd=1, relief="groove")
        res_container.pack(side="left", fill="both", expand=True)
        tk.Label(res_container, text="Results", font=("Arial", 12, "bold")).pack(anchor="nw", pady=5)
        self._res_canvas = tk.Canvas(res_container)
        self._res_canvas.pack(side="left", fill="both", expand=True)
        res_scroll = tk.Scrollbar(res_container, orient="vertical", command=self._res_canvas.yview)
        res_scroll.pack(side="right", fill="y")
        self._res_canvas.configure(yscrollcommand=res_scroll.set)
        self.res_frame = tk.Frame(self._res_canvas)
        self._res_canvas.create_window((0,0), window=self.res_frame, anchor="nw")
        self.res_frame.bind("<Configure>", lambda e: self._res_canvas.configure(scrollregion=self._res_canvas.bbox("all")))

    def show_login_screen(self):
        print("Showing login screen")
        self.search_frame.pack_forget()
        self.login_frame.pack(fill="both", expand=True)

    def show_search_screen(self):
        print("Showing search screen")
        self.login_frame.pack_forget()
        self.search_frame.pack(fill="both", expand=True)

    def _on_forgot(self):
        print("Forgot password? button pressed")

    def _toggle_signup(self):
        self.signup_mode_enabled = not self.signup_mode_enabled
        mode = "Sign up" if self.signup_mode_enabled else "Login"
        print(f"Toggling signup mode: now in {mode} mode")
        self.login_label.config(text=mode)
        self.signup_button.config(text="Back to Login" if self.signup_mode_enabled else "Sign up!")
        self.login_button.config(text="SIGN UP" if self.signup_mode_enabled else "LOGIN")

    def _on_login(self):
        u = self.username_var.get().strip()
        p = self.password_var.get().strip()
        print("LOGIN button pressed")
        print(f"  Username: {u!r}")
        print(f"  Password: {p!r}")
        if u and p:
            self.credentials["username"] = u
            self.credentials["password"] = p
            self.show_search_screen()

    def _on_back(self):
        print("Back button pressed")
        self.query_entry.delete(0, "end")
        for w in self.attr_frame.winfo_children():
            w.destroy()
        for w in self.res_frame.winfo_children():
            w.destroy()
        self.show_login_screen()

    def _on_search(self):
        q = self.query_entry.get().strip()
        print(f"Search button pressed with query: {q!r}")
        if not q:
            return
        messagebox.showinfo("Search", "Starting the search, this might take some time.")
        try:
            resp = handle_request({"type": "inputFromUI", "query": q, "k": 5})
            results = resp.get("results", [])
        except Exception as e:
            print("Error during handle_request:", e)
            results = []

        # update attributes panel with mock data
        for w in self.attr_frame.winfo_children():
            w.destroy()
        attrs = {
            "Total Emails": random.randint(100, 500),
            "Total Conversations": random.randint(10, 100),
            "Last Query": q,
            "Example Attr": "Value"
        }
        for k, v in attrs.items():
            lbl = tk.Label(self.attr_frame, text=f"{k}: {v}", anchor="w")
            lbl.pack(fill="x", padx=5, pady=2)

        # update results panel
        for w in self.res_frame.winfo_children():
            w.destroy()
        if not results:
            lbl = tk.Label(
                self.res_frame,
                text="I couldn't find any emails relating to your query",
                font=("Arial", 14),
                wraplength=800,
                justify="center"
            )
            lbl.pack(pady=20)
        else:
            for r in results:
                card = tk.Frame(self.res_frame, bd=1, relief="solid", padx=5, pady=5)
                header = tk.Label(
                    card,
                    text=f"{r.get('subject','No Subject')}  ({r.get('score',0):.3f})",
                    font=("Arial", 12, "bold"),
                    anchor="w"
                )
                header.pack(fill="x")
                info = tk.Label(
                    card,
                    text=f"From: {r.get('from','unknown')}    Date: {r.get('date','')}",
                    anchor="w"
                )
                info.pack(fill="x", pady=(2,5))

                content = r.get("content","")
                snippet = content[:200]
                var = tk.StringVar(value=snippet)
                lbl = tk.Label(card, textvariable=var, wraplength=800, justify="left", anchor="w")
                lbl.pack(fill="x")

                if len(content) > 200:
                    def make_toggle(v=var, full=content):
                        def toggle():
                            if v.get() == snippet:
                                print("Read more clicked: expanding content")
                                v.set(full)
                            else:
                                print("Read more clicked: collapsing content")
                                v.set(snippet)
                        return toggle

                    btn = tk.Button(card, text="Read more", command=make_toggle())
                    btn.pack(anchor="e", pady=(5,0))

                card.pack(fill="x", padx=5, pady=5)

    def run(self):
        self.root.mainloop()

if __name__ == "__main__":
    App().run()
