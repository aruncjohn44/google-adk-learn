function setupChat() {
  const input = document.getElementById("queryInput");
  input.addEventListener("keypress", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      sendQuery();
    }
  });
}

async function sendQuery() {
  const input = document.getElementById("queryInput");
  const query = input.value.trim();
  if (!query) return;

  input.value = "";

  const messages = document.getElementById("messages");

  messages.innerHTML += `
    <div class="message user-message">
      <div class="label">You</div>
      <div class="bubble">${query}</div>
    </div>
  `;

  const resp = await fetch("/ask", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query })
  });

  const data = await resp.json();

  let tableHtml = data.data ? renderTable(data.data) : "";

  messages.innerHTML += `
    <div class="message assistant-message">
      <div class="label">Assistant</div>
      <div class="bubble">${data.answer || "No answer"}${tableHtml}</div>
    </div>
  `;

  messages.scrollTop = messages.scrollHeight;
}

function renderTable(rows) {
  if (!rows.length) return "";

  const cols = Object.keys(rows[0]);
  let table = "<table><tr>";
  cols.forEach(c => table += `<th>${c}</th>`);
  table += "</tr>";

  rows.forEach(r => {
    table += "<tr>";
    cols.forEach(c => table += `<td>${r[c]}</td>`);
    table += "</tr>";
  });

  table += "</table>";
  return table;
}
