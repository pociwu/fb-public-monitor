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
  document.addEventListener("htmx:afterSwap", (event) => initExpandable(event.target));
  window.addEventListener("load", () => initExpandable());
})();
