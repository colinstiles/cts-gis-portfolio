# GIS Portfolio

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This is my [GIS portfolio](https://colinstiles.github.io/cts-gis-portfolio) hosted on GitHub Pages. It is created with CSS, HTML, and JavaScript as an easily customizable, lightweight, and fully responsive static site.

## Contents

- [GIS Portfolio](#gis-portfolio)
  - [Contents](#contents)
    - [Customization](#customization)
    - [Images](#images)
    - [Header Section](#header-section)
    - [Lead Section](#lead-section)
    - [About Section](#about-section)
    - [Experience Section](#experience-section)
    - [Education Section](#education-section)
    - [Projects Section](#projects-section)
    - [Affiliations Section](#affiliations-section)
    - [Skills Section](#skills-section)
    - [Contact Section](#contact-section)
    - [Footer Section](#footer-section)
  - [License](#license)

### Customization
All of the portfolio content is stored in the `index.html` file. The styles are in `css/styles.css` and the JavaScript is in `js/scripts.js`. To customize, simply edit these files and push the changes to your repository.

### Images

The portfolio contains the following images:

* Main background (images/lead-bg.jpg) - this is the main background image. Created in ArcGIS Pro.
* Favicon (/favicon.ico) - this is the favicon used for the page. It is a custom image spun off from my `tin+topo` logo.
* Project image - these are the images associated with my projects under the project section.

### Header Section

The header section can be found within the `<header>` tag and simply contains an unordered list of anchors to different sections of the page.

My header includes:

```HTML
<ul>
    <li>
        <a href="#about">About</a>
    </li>
    <li>
        <a href="#experience">Experience</a>
    </li>
    <li>
        <a href="#education">Education</a>
    </li>
    <li>
        <a href="#projects">Projects</a>
    </li>
    <li>
        <a href="#affiliations">Affiliations</a>
    </li>
    <li>
        <a href="#skills">Skills</a>
    </li>
    <li>
        <a href="#contact">Contact</a>
    </li>
</ul>
```

### Lead Section

The Lead section simply includes a header for my name and title, as well as a link that will download my resume.

### About Section

The About section contains a quick about blurb.

### Experience Section

The Experience section is a vertical timeline with my recent jobs.

### Education Section

The Education is a series of `.education-block` classes with details about school, degree, and pertinent courses taken.

### Projects Section

The Project section contains a number of `.project` elements that represent some of my projects:
*   Python data pipelines and ArcGIS Online administration notebooks
*   Interactive web applications and dashboards built with ArcGIS Experience Builder and Dashboards
*   Data-driven narratives using ArcGIS StoryMaps
*   A gallery of static cartographic maps
*   A collection of my technical articles on Medium

### Affiliations Section

The Affiliations section lists the organizations that I am a member of or have affiliation with.

### Skills Section
The Skills section is an unordered list that creates a "Skill Cloud" with some of my technical skills:

```HTML
<ul>
    <li>ArcGIS Enterprise</li>
    <li>ArcGIS Online</li>     
    <li>ArcGIS Pro</li>
    <li>Python</li>
	<li>SQL</li>
	<li>R</li>
    <li>Experience Builder</li>
    <li>Field Maps</li>
    <li>Dashboards</li>
    <li>Story Maps</li>
    <li>QGIS</li>
    <li>PostGIS</li>
    <li>PostgreSQL</li>
    <li>Arcade</li>
    <li>RESTful APIs</li>
    <li>GPS Data Collection</li>
    <li>Blender 3D</li>
    <li>Google Colab</li>
    <li>Google Cloud</li>
    <li>CSS</li>
    <li>SCSS</li>
    <li>JavaScript</li>
    <li>HTML</li>
</ul>
```

### Contact Section

The Contact section includes a link to my personal email.

### Footer Section

The Footer contains a list of my social and professional profiles, which includes:

*   [Personal GitHub](https://github.com/colinstiles)
*   [Work GitHub](https://github.com/colin-tda)
*   [Org GitHub](https://github.com/tn-dept-ag)
*   [LinkedIn](https://www.linkedin.com/in/colin-t-stiles-gisp-386717292/)
*   [Medium](https://www.medium.com/@colintimothystiles)

## License

This project is open source and available under the [MIT License](LICENSE.md).
