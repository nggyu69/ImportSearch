// static/js/awaves.js

class AWaves extends HTMLElement {
    connectedCallback() {
      // Get the svg element inside our custom element.
      this.svg = this.querySelector('.js-svg');
  
      // Set up the mouse and animation properties.
      this.mouse = {
        x: -10,
        y: 0,
        lx: 0,
        ly: 0,
        sx: 0,
        sy: 0,
        v: 0,
        vs: 0,
        a: 0,
        set: false,
      };
  
      this.lines = [];
      this.paths = [];
      this.noise = new Noise(Math.random()); // Provided by noise.js
  
      // Initialize size and lines
      this.setSize();
      this.setLines();
  
      // Bind event listeners for resize and mouse/touch events
      this.bindEvents();
  
      // Start animation loop
      requestAnimationFrame(this.tick.bind(this));
    }
  
    bindEvents() {
      window.addEventListener('resize', this.onResize.bind(this));
      window.addEventListener('mousemove', this.onMouseMove.bind(this));
      this.addEventListener('touchmove', this.onTouchMove.bind(this));
    }
  
    onResize() {
      this.setSize();
      this.setLines();
    }
  
    onMouseMove(e) {
      this.updateMousePosition(e.pageX, e.pageY);
    }
  
    onTouchMove(e) {
      e.preventDefault();
      const touch = e.touches[0];
      this.updateMousePosition(touch.clientX, touch.clientY);
    }
  
    updateMousePosition(x, y) {
      const { mouse } = this;
      mouse.x = x - this.bounding.left;
      mouse.y = y - this.bounding.top + window.scrollY;
      if (!mouse.set) {
        mouse.sx = mouse.x;
        mouse.sy = mouse.y;
        mouse.lx = mouse.x;
        mouse.ly = mouse.y;
        mouse.set = true;
      }
    }
  
    setSize() {
      this.bounding = this.getBoundingClientRect();
      this.svg.style.width = `${this.bounding.width}px`;
      this.svg.style.height = `${this.bounding.height}px`;
    }
  
    setLines() {
      const { width, height } = this.bounding;
      this.lines = [];
  
      // Remove previous paths if any.
      this.paths.forEach((path) => path.remove());
      this.paths = [];
  
      const xGap = 10, yGap = 32;
      const oWidth = width + 200, oHeight = height + 30;
      const totalLines = Math.ceil(oWidth / xGap);
      const totalPoints = Math.ceil(oHeight / yGap);
      const xStart = (width - xGap * totalLines) / 2;
      const yStart = (height - yGap * totalPoints) / 2;
  
      for (let i = 0; i <= totalLines; i++) {
        const points = [];
        for (let j = 0; j <= totalPoints; j++) {
          points.push({
            x: xStart + xGap * i,
            y: yStart + yGap * j,
            wave: { x: 0, y: 0 },
            cursor: { x: 0, y: 0, vx: 0, vy: 0 },
          });
        }
        // Create an SVG path element for this line.
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.classList.add('a__line', 'js-line');
        this.svg.appendChild(path);
        this.paths.push(path);
        this.lines.push(points);
      }
    }
  
    movePoints(time) {
      const { lines, mouse, noise } = this;
      lines.forEach((points) => {
        points.forEach((p) => {
          // Wave movement based on Perlin noise.
          const move = noise.perlin2(
            (p.x + time * 0.0125) * 0.002,
            (p.y + time * 0.005) * 0.0015
          ) * 12;
          p.wave.x = Math.cos(move) * 32;
          p.wave.y = Math.sin(move) * 16;
  
          // Mouse interaction effect.
          const dx = p.x - mouse.sx;
          const dy = p.y - mouse.sy;
          const d = Math.hypot(dx, dy);
          const l = Math.max(175, mouse.vs);
          if (d < l) {
            const s = 1 - d / l;
            const f = Math.cos(d * 0.001) * s;
            p.cursor.vx += Math.cos(mouse.a) * f * l * mouse.vs * 0.00065;
            p.cursor.vy += Math.sin(mouse.a) * f * l * mouse.vs * 0.00065;
          }
  
          // Apply friction and tension.
          p.cursor.vx += (0 - p.cursor.x) * 0.005;
          p.cursor.vy += (0 - p.cursor.y) * 0.005;
          p.cursor.vx *= 0.925;
          p.cursor.vy *= 0.925;
          p.cursor.x += p.cursor.vx * 2;
          p.cursor.y += p.cursor.vy * 2;
          p.cursor.x = Math.min(100, Math.max(-100, p.cursor.x));
          p.cursor.y = Math.min(100, Math.max(-100, p.cursor.y));
        });
      });
    }
  
    moved(point, withCursorForce = true) {
      const coords = {
        x: point.x + point.wave.x + (withCursorForce ? point.cursor.x : 0),
        y: point.y + point.wave.y + (withCursorForce ? point.cursor.y : 0),
      };
      // Round coordinates for smoother rendering.
      coords.x = Math.round(coords.x * 10) / 10;
      coords.y = Math.round(coords.y * 10) / 10;
      return coords;
    }
  
    drawLines() {
      const { lines, moved, paths } = this;
      lines.forEach((points, lIndex) => {
        let p1 = moved(points[0], false);
        let d = `M ${p1.x} ${p1.y}`;
        points.forEach((p1, pIndex) => {
          const isLast = pIndex === points.length - 1;
          p1 = moved(p1, !isLast);
          d += `L ${p1.x} ${p1.y}`;
        });
        paths[lIndex].setAttribute('d', d);
      });
    }
  
    tick(time) {
      const { mouse } = this;
      mouse.sx += (mouse.x - mouse.sx) * 0.1;
      mouse.sy += (mouse.y - mouse.sy) * 0.1;
      const dx = mouse.x - mouse.lx;
      const dy = mouse.y - mouse.ly;
      const d = Math.hypot(dx, dy);
      mouse.v = d;
      mouse.vs += (d - mouse.vs) * 0.1;
      mouse.vs = Math.min(100, mouse.vs);
      mouse.lx = mouse.x;
      mouse.ly = mouse.y;
      mouse.a = Math.atan2(dy, dx);
  
      // Update CSS custom properties for potential CSS effects.
      this.style.setProperty('--x', `${mouse.sx}px`);
      this.style.setProperty('--y', `${mouse.sy}px`);
  
      this.movePoints(time);
      this.drawLines();
      requestAnimationFrame(this.tick.bind(this));
    }
  }
  
  customElements.define('a-waves', AWaves);
  