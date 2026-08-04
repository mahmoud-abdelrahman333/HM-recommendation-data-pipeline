async function getRecommendations() {
    const customerId = document.getElementById("customerId").value.trim();

    const error = document.getElementById("error");
    const results = document.getElementById("results");
    const grid = document.getElementById("recommendationGrid");
    const customerLabel = document.getElementById("customerLabel");

    error.textContent = "";

    if (!customerId) {
        error.textContent = "Please enter a customer ID.";
        return;
    }

    results.classList.remove("hidden");

    grid.innerHTML = `
        <div class="product">
            <p>Loading recommendations...</p>
        </div>
    `;

    try {
        const response = await fetch(
            `/api/recommendations/${encodeURIComponent(customerId)}`
        );

        if (!response.ok) {
            throw new Error(`API returned ${response.status}`);
        }

        const data = await response.json();

        console.log("API DATA:", data);

        if (!data.recommendations || data.recommendations.length === 0) {
            throw new Error("No recommendations found.");
        }

        customerLabel.textContent = `Customer: ${customerId}`;

        grid.innerHTML = data.recommendations.map(item => {
            const productUrl =
    `https://www.google.com/search?udm=14&q=${encodeURIComponent(
        "H&M " + item.prod_name
    )}`;

            return `
                <div class="product">
                    <div class="rank">#${item.rank}</div>

                    <h3>${item.prod_name || "Recommended Product"}</h3>

                    <p>${item.product_type || ""}</p>

                    <p>${item.product_group || ""}</p>

                    <p>${item.department || ""}</p>

                    <div class="score">
                        Score:
                        <strong>
                            ${Number(item.prediction || 0).toFixed(4)}
                        </strong>
                    </div>

                    <a
                        href="${productUrl}"
                        target="_blank"
                        rel="noopener noreferrer"
                        class="product-link"
                    >
                        Search Product ↗
                    </a>
                </div>
            `;
        }).join("");

    } catch (errorObject) {
        console.error(errorObject);

        results.classList.add("hidden");
        error.textContent = errorObject.message;
    }
}

document
    .getElementById("customerId")
    .addEventListener("keydown", event => {
        if (event.key === "Enter") {
            getRecommendations();
        }
    });
