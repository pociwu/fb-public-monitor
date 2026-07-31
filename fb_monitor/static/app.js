(() => {
  const lightbox = document.getElementById("lightbox");
  if (!lightbox) return;
  const image = lightbox.querySelector("img");
  const download = lightbox.querySelector(".lightbox-download");
  let items = [];
  let current = 0;
  let touchStartX = 0;

  function show(index) {
    if (!items.length) return;
    current = (index + items.length) % items.length;
    image.src = items[current].dataset.lightboxSrc;
    download.href = items[current].dataset.download || image.src;
    lightbox.querySelector(".lightbox-prev").hidden = items.length < 2;
    lightbox.querySelector(".lightbox-next").hidden = items.length < 2;
  }

  function open(trigger) {
    const gallery = trigger.closest("[data-gallery]");
    items = gallery ? [...gallery.querySelectorAll("[data-lightbox-src]")] : [trigger];
    current = Math.max(0, items.indexOf(trigger));
    show(current);
    lightbox.hidden = false;
    document.body.classList.add("lightbox-open");
    lightbox.querySelector(".lightbox-close").focus();
  }

  function close() {
    lightbox.hidden = true;
    image.removeAttribute("src");
    document.body.classList.remove("lightbox-open");
  }

  function initExpandable(root = document) {
    root.querySelectorAll("[data-expandable]").forEach((block) => {
      const toggle = block.nextElementSibling;
      if (!toggle || !toggle.classList.contains("text-toggle")) return;
      toggle.hidden = block.scrollHeight <= block.clientHeight + 2;
    });
  }

  function initProfileSorting(root = document) {
    const containers = [];
    if (root.matches?.("[data-profile-cards]")) containers.push(root);
    containers.push(...root.querySelectorAll("[data-profile-cards]"));
    containers.forEach((container) => {
      if (container.dataset.sortInitialized) return;
      container.dataset.sortInitialized = "true";
      let dragged = null;
      let changed = false;

      container.addEventListener("dragstart", (event) => {
        dragged = event.target.closest("[data-profile-id]");
        if (!dragged) return;
        changed = false;
        dragged.classList.add("is-dragging");
        document.body.classList.add("profile-drag-active");
        event.dataTransfer.effectAllowed = "move";
        event.dataTransfer.setData("text/plain", dragged.dataset.profileId);
      });

      container.addEventListener("dragover", (event) => {
        if (!dragged) return;
        const target = event.target.closest("[data-profile-id]");
        if (!target || target === dragged) return;
        event.preventDefault();
        const rect = target.getBoundingClientRect();
        const sameRow = event.clientY >= rect.top && event.clientY <= rect.bottom;
        const insertAfter = sameRow
          ? event.clientX > rect.left + rect.width / 2
          : event.clientY > rect.top + rect.height / 2;
        container.insertBefore(dragged, insertAfter ? target.nextSibling : target);
        changed = true;
      });

      container.addEventListener("drop", (event) => event.preventDefault());
      container.addEventListener("dragend", async () => {
        if (!dragged) return;
        dragged.classList.remove("is-dragging");
        document.body.classList.remove("profile-drag-active");
        dragged = null;
        if (!changed) return;
        const profileIds = [...container.querySelectorAll("[data-profile-id]")]
          .map((card) => Number(card.dataset.profileId));
        try {
          const response = await fetch("/profiles/reorder", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({profile_ids: profileIds}),
          });
          if (!response.ok) throw new Error(`HTTP ${response.status}`);
          container.classList.add("order-saved");
          setTimeout(() => container.classList.remove("order-saved"), 600);
        } catch (error) {
          window.alert(`排序儲存失敗：${error.message}`);
          window.location.reload();
        }
      });
    });
  }

  document.addEventListener("click", (event) => {
    const preview = event.target.closest("[data-lightbox-src]");
    if (preview) {
      event.preventDefault();
      open(preview);
      return;
    }
    if (event.target.closest(".lightbox-close") || event.target === lightbox) close();
    if (event.target.closest(".lightbox-prev")) show(current - 1);
    if (event.target.closest(".lightbox-next")) show(current + 1);
    const toggle = event.target.closest(".text-toggle");
    if (toggle) {
      const text = toggle.previousElementSibling;
      text.classList.toggle("expanded");
      toggle.textContent = text.classList.contains("expanded") ? "收合" : "顯示更多";
    }
  });

  document.addEventListener("keydown", (event) => {
    if (lightbox.hidden) return;
    if (event.key === "Escape") close();
    if (event.key === "ArrowLeft") show(current - 1);
    if (event.key === "ArrowRight") show(current + 1);
  });
  lightbox.addEventListener("touchstart", (event) => { touchStartX = event.changedTouches[0].clientX; }, { passive: true });
  lightbox.addEventListener("touchend", (event) => {
    const delta = event.changedTouches[0].clientX - touchStartX;
    if (Math.abs(delta) > 50) show(current + (delta < 0 ? 1 : -1));
  }, { passive: true });
  document.addEventListener("htmx:afterSwap", (event) => {
    initExpandable(event.target);
    initProfileSorting(event.target);
  });
  window.addEventListener("load", () => {
    initExpandable();
    initProfileSorting();
  });
})();
